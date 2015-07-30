package Queue::Q::ReliableFIFO::RedisNG2000TopFun;

use strict;
use warnings;

use Carp qw/croak/;
use Digest::SHA qw//;
use Redis qw//;
use Time::HiRes qw//;

use Queue::Q::ReliableFIFO::ItemNG2000TopFun;

use Class::XSAccessor {
    getters => [ qw/
      server
      port
      db_id
      queue_name
      busy_expiry_time
      claim_wait_timeout
      requeue_limit
      redis_handle
      redis_options
      warn_on_requeue
      _pending_queue
      _busy_queue
      _retry_queue
      _failed_queue
      _temp_queue
      _script_cache
      _lua
    /],
    setters => {
        set_requeue_limit      => 'requeue_limit',
        set_busy_expiry_time   => 'busy_expiry_time',
        set_claim_wait_timeout => 'claim_wait_timeout',
    }
};

use constant { 
    USE_BLOCKING_CLAIM => 1,
};

my %VALID_SUBQUEUES = map { $_ => 0 } qw/
    pending
    busy
    retry
    failed
    temp
/;

my %VALID_PARAMS = map { $_ => 1 } qw/
    server
    port
    db
    queue_name
    busy_expiry_time
    claim_wait_timeout
    requeue_limit
    redis_handle
    redis_options
    warn_on_requeue
/;

sub new {
    my ($class, %params) = @_;

    foreach my $required_param (qw/ server port queue_name /) {
        $params{$required_param} or croak __PACKAGE__ . "->new: missing mandatory parameter $required_param";
    }

    foreach my $provided_param (keys %params) {
        exists $VALID_PARAMS{$provided_param} or croak __PACKAGE__ ."->new: encountered unknown parameter $provided_param";
    }

    my $self = bless({
        requeue_limit      => 5,
        busy_expiry_time   => 30,
        claim_wait_timeout => 1,
        db_id              => 0,
        warn_on_requeue    => 0,
        %params
    } => $class);

    my %default_redis_options = (
        reconnect => 60,
        encoding  => undef, # force undef for binary data
        server    => join( ':' => $params{server}, $params{port} ),
    );

    foreach my $subqueue_name ( keys %VALID_SUBQUEUES ) {
        my $accessor_name = sprintf '_%s_%s', $subqueue_name, 'queue';
        my $redis_list_name = sprintf '%s_%s', $params{queue_name}, $subqueue_name;
        $self->{$accessor_name} = $redis_list_name;
    }

    my %redis_options = %{ $params{redis_options} || {} };

    $self->{redis_handle} //= Redis->new(
        %default_redis_options, %redis_options,
    );

    $self->redis_handle->select($params{db_id}) if $params{db_id};

    return $self;
}

sub enqueue_item {
    my $self = shift;

    return unless @_;

    if ( grep { ref $_ } @_ ) {
        die sprintf '%s->enqueue_item: encountered a reference; all payloads must be serialised in string format', __PACKAGE__;
    }

    my $redis_handle = $self->redis_handle;

    my $pushed = 0;

    foreach my $item (@_) {
        my $now = Time::HiRes::time;
        my $sha512 = Digest::SHA::sha512_hex($item);
        my $item_key = sprintf '%s-%s-%s', $self->queue_name, $sha512, $now;

        # create payload item
        my $setnx_success = $redis_handle->setnx("item-$item_key" => $item);

        my %meta_payload = (
            process_count => 0, # amount of times a single consumer attempted to handle the item
            bail_count    => 0, # amount of times process_count exceeded its threshold
            time_created  => $now,
            time_enqueued => $now,
        );

        # create metadata
        my $hmset_success = $redis_handle->hmset( "meta-$item_key" => %meta_payload );

        require Data::Dumper;
        warn Data::Dumper::Dumper({
            pending_queue => $self->_pending_queue,
            item_key => $item_key,
        });

        # enqueue item
        unless ( $redis_handle->lpush( $self->_pending_queue, $item_key ) ) {
            die sprintf '%s->enqueue_item failed to lpush key %s onto pending queue %s', __PACKAGE__, $item_key, $self->_pending_queue;
        }

        $pushed++;
    }
}

sub claim_item {
    my ($self, $n_items) = @_;
    $self->_claim_item_internal($n_items);
}


sub claim_item_nonblocking {
    my ($self, $n_items) = @_;
    $self->_claim_item_internal($n_items, USE_BLOCKING_CLAIM);
}

sub _claim_item_internal {
    my ($self, $n_items, $do_blocking) = @_;

    my $timeout       = $self->claim_wait_timeout;
    my $redis_handle  = $self->redis_handle;
    my $pending_queue = $self->_pending_queue;
    my $busy_queue    = $self->_busy_queue;

    unless ( defined $n_items and $n_items > 0 ) {
        $n_items = 1;
    }

    if ( $n_items == 1 and !$do_blocking ) {
        return unless my $key = $redis_handle->rpoplpush($self->_pending_queue, $self->_busy_queue);

        my %metadata = $redis_handle->hgetall("meta-$key");
        my $payload  = $redis_handle->get("item-$key");

        return Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
            key      => $key,
            payload  => $payload,
            metadata => \%metadata,
        });
    }
    elsif ( $n_items == 1 and $do_blocking ) {
        my $key = $redis_handle->rpoplpush($self->_pending_queue, $self->_busy_queue) 
               || $redis_handle->brpoplpush($self->_pending_queue, $self->_busy_queue);

        return unless $key;

        my %metadata = $redis_handle->hgetall("meta-$key");
        my $payload  = $redis_handle->get("item-$key");

        return Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
            key      => $key,
            payload  => $payload,
            metadata => \%metadata,
        });
    }
    else {

        # when fetching multiple items:
        # - in non-blocking mode, we try to fetch one item, and then give up.
        # - in blocking mode, we attempt to fetch the first item using
        #   brpoplpush, and if it succeeds, we switch to rpoplpush for greater
        #   throughput.

        my @items;

        my $handler = sub {
            return unless defined(my $key = $_[0]);

            my %metadata = $redis_handle->hgetall("meta-$key");
            my $payload  = $redis_handle->get("item-$key");

            unless ( keys %metadata ) {
                warn sprintf '%s->_claim_item_internal: fetched empty metadata for key %s', __PACKAGE__, $key;
            }
            unless ( defined $payload ) {
                warn sprintf '%s->_claim_item_internal: fetched empty payload for key %s', __PACKAGE__, $key;
            }

            push @items, Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
                key      => $key,
                payload  => $payload,
                metadata => \%metadata,
            });
        };

        my $first_item;

        if ($n_items > 30) {
            # yes, there is a race, but it's an optimization only
            my ($llen) = $redis_handle->llen($pending_queue);
            $n_items = $llen if $llen < $n_items;
        }
        eval {
            $redis_handle->rpoplpush($pending_queue, $busy_queue, $handler) for 1 .. $n_items;
            $redis_handle->wait_all_responses;
            if ( @items == 0 && $do_blocking ) {
                $first_item = $redis_handle->brpoplpush($pending_queue, $busy_queue, $timeout);

                if (defined $first_item) {
                    $handler->($first_item);
                    $redis_handle->rpoplpush($pending_queue, $busy_queue, $handler) for 1 .. ($n_items-1);
                    $redis_handle->wait_all_responses;
                }
            }
            1;
        } or do {
            my $eval_error = $@ || 'zombie error';
            warn sprintf '%s->_claim_item_internal encountered an exception while claiming bulk items: %s', __PACKAGE__, $eval_error;
        };

        return @items;
    }

    die sprintf '%s->_claim_item_internal: how did we end up here?', __PACKAGE__;
}

sub finish_item {
    my ($self, $items) = @_;

    my $redis_handle = $self->redis_handle;

    my $items_marked = 0;

    foreach my $item (@$items) {
        $redis_handle->lrem( $self->_busy_queue, -1, $item->{key}, sub { $items_marked += $_[0] } );
    }

    $redis_handle->wait_all_responses;

    if ( $items_marked != @$items ) {
        warn sprintf '%s->mark_item_as_done: $items_marked != @$items (did some items get stuck?)', __PACKAGE__;
    }

    return $items_marked;
}

1;
