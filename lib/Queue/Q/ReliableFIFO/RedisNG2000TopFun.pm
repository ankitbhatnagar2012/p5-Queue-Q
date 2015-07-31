package Queue::Q::ReliableFIFO::RedisNG2000TopFun;

use strict;
use warnings;

use Data::Dumper;
use Carp qw/croak cluck/;
use Data::UUID::MT;
use Redis qw//;
use Time::HiRes qw//;

use Queue::Q::ReliableFIFO::Lua;
use Queue::Q::ReliableFIFO::ItemNG2000TopFun;

use constant {
    CLAIM_NONBLOCKING => 0,
    CLAIM_BLOCKING => 1,
};

our ( %VALID_SUBQUEUES, %VALID_PARAMS );

BEGIN {
    # define subqueue names and map to accessors
    %VALID_SUBQUEUES = map { $_ => "_${_}_queue" } qw/
        unprocessed
        working
        processed
        failed
        temp
    /;

    # valid constructor params
    %VALID_PARAMS = map { $_ => 1 } qw/
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
}

use Class::XSAccessor {
    getters => [
        values %VALID_SUBQUEUES, qw/
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
            _script_cache
            _lua
    /],
    setters => {
        set_requeue_limit      => 'requeue_limit',
        set_busy_expiry_time   => 'busy_expiry_time',
        set_claim_wait_timeout => 'claim_wait_timeout',
    }
};
my $UUID = Data::UUID::MT->new( version => '4s' );

sub new {
    my ($class, %params) = @_;

    foreach my $required_param (qw/ server port queue_name /) {
        $params{$required_param} or croak __PACKAGE__ . "->new: missing mandatory parameter $required_param";
    }

    foreach my $provided_param (keys %params) {
        $VALID_PARAMS{$provided_param} or croak __PACKAGE__ ."->new: encountered unknown parameter $provided_param";
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

    # populate subqueue attributes with name of redis list
    # e.g. unprocessed -> _unprocessed_queue (accessor name) -> foo_unprocessed (redis list name)
    foreach my $subqueue_name ( keys %VALID_SUBQUEUES ) {
        my $accessor_name = $VALID_SUBQUEUES{$subqueue_name};
        my $redis_list_name = sprintf '%s_%s', $params{queue_name}, $subqueue_name;
        $self->{$accessor_name} = $redis_list_name;
    }

    my %redis_options = %{ $params{redis_options} || {} };

    $self->{redis_handle} //= Redis->new(
        %default_redis_options, %redis_options,
    );

    $self->{_lua} = Queue::Q::ReliableFIFO::Lua->new(
        redis_conn => $self->redis_handle,
    );

    $self->redis_handle->select($params{db_id}) if $params{db_id};

    return $self;
}

sub enqueue_item {
    my $self = shift;

    my $items = ( @_ == 1 and ref $_[0] eq 'ARRAY')
        ? $_[0]
        : \@_; # or should we copy it instead?

    if ( grep { ref $_ } @$items ) {
        die sprintf '%s->enqueue_item: encountered a reference; all payloads must be serialised in string format', __PACKAGE__;
    }

    my $redis_handle = $self->redis_handle;

    my @created;

    foreach my $input_item (@$items) {
        my $item_id  = $UUID->create_hex();
        my $item_key = sprintf '%s-%s', $self->queue_name, $item_id;

        # create payload item
        my $setnx_success = $redis_handle->setnx("item-$item_key" => $input_item);

        my $now = Time::HiRes::time;

        my %metadata = (
            process_count => 0, # amount of times a single consumer attempted to handle the item
            bail_count    => 0, # amount of times process_count exceeded its threshold
            time_created  => $now,
            time_enqueued => $now,
        );

        # create metadata
        my $hmset_success = $redis_handle->hmset( "meta-$item_key" => %metadata );

        # enqueue item
        unless ( $redis_handle->lpush( $self->_unprocessed_queue, $item_key ) ) {
            die sprintf '%s->enqueue_item failed to lpush item_key=%s onto unprocessed queue %s', __PACKAGE__, $item_key, $self->_unprocessed_queue;
        }

        push @created, Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
            item_key => $item_key,
            payload  => $input_item,
            metadata => \%metadata,
        });
    }

    return \@created;
}

sub claim_item {
    my ($self, $n_items) = @_;
    $self->_claim_item_internal($n_items, CLAIM_BLOCKING);
}

sub claim_item_nonblocking {
    my ($self, $n_items) = @_;
    $self->_claim_item_internal($n_items, CLAIM_NONBLOCKING);
}

sub _claim_item_internal {
    my ($self, $n_items, $do_blocking) = @_;

    my $timeout           = $self->claim_wait_timeout;
    my $redis_handle      = $self->redis_handle;
    my $unprocessed_queue = $self->_unprocessed_queue;
    my $working_queue        = $self->_working_queue;

    unless ( defined $n_items and $n_items > 0 ) {
        $n_items = 1;
    }

    if ( $n_items == 1 and $do_blocking == CLAIM_NONBLOCKING ) {
        return unless my $item_key = $redis_handle->rpoplpush($self->_unprocessed_queue, $self->_working_queue);

        my %metadata = $redis_handle->hgetall("meta-$item_key");
        my $payload  = $redis_handle->get("item-$item_key");

        return Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
            item_key      => $item_key,
            payload  => $payload,
            metadata => \%metadata,
        });
    }
    elsif ( $n_items == 1 and $do_blocking == CLAIM_BLOCKING ) {
        my $item_key = $redis_handle->rpoplpush($self->_unprocessed_queue, $self->_working_queue)
               || $redis_handle->brpoplpush($self->_unprocessed_queue, $self->_working_queue, $self->claim_wait_timeout);

        return unless $item_key;

        my %metadata = $redis_handle->hgetall("meta-$item_key");
        my $payload  = $redis_handle->get("item-$item_key");

        return Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
            item_key => $item_key,
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
            return unless defined(my $item_key = $_[0]);

            my %metadata = $redis_handle->hgetall("meta-$item_key");
            my $payload  = $redis_handle->get("item-$item_key");

            unless ( keys %metadata ) {
                warn sprintf '%s->_claim_item_internal: fetched empty metadata for item_key=%s', __PACKAGE__, $item_key;
            }
            unless ( defined $payload ) {
                warn sprintf '%s->_claim_item_internal: fetched empty payload for item_key=%s', __PACKAGE__, $item_key;
            }

            push @items, Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
                item_key => $item_key,
                payload  => $payload,
                metadata => \%metadata,
            });
        };

        my $first_item;

        if ($n_items > 30) {
            # yes, there is a race, but it's an optimization only
            my ($llen) = $redis_handle->llen($unprocessed_queue);
            $n_items = $llen if $llen < $n_items;
        }
        eval {
            $redis_handle->rpoplpush($unprocessed_queue, $working_queue, $handler) for 1 .. $n_items;
            $redis_handle->wait_all_responses;
            if ( @items == 0 && $do_blocking ) {
                $first_item = $redis_handle->brpoplpush($unprocessed_queue, $working_queue, $timeout);

                if (defined $first_item) {
                    $handler->($first_item);
                    $redis_handle->rpoplpush($unprocessed_queue, $working_queue, $handler) for 1 .. ($n_items-1);
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

sub mark_item_as_processed {
    my $self = shift;

    my $items = ( @_ == 1 and ref $_[0] eq 'ARRAY')
        ? $_[0]
        : \@_; # or should we copy it instead?

    my $redis_handle = $self->redis_handle;

    my %result = (
        flushed => [],
        failed  => [],
    );

    # callback receives result of LREM call for removing item from the working
    # queue and populates %result. if the LREM succeeds, we also need to clean
    # up the payload+metadata.

    foreach my $item (@$items) {
        my $item_key = $item->{item_key};
        my $lrem_direction = 1; # head-to-tail (http://redis.io/commands/lrem)

        $redis_handle->lrem(
            $self->_working_queue,
            $lrem_direction,
            $item_key,
            sub {
                my $result_key = $_[0] ? 'flushed' : 'failed';
                push @{ $result{$result_key} } => $item_key;
            }
        );
    }

    $redis_handle->wait_all_responses;

    my ($flushed, $failed) = @result{qw/flushed failed/};

    my @to_purge = @$flushed;

    while (@to_purge) {
        my @chunk = map  {; ("meta-$_" => "item-$_") }
                    grep { defined $_ }
                    splice @to_purge, 0, 100;

        # warn Dumper({ delete_chunk => \@chunk });
        my $deleted;
        $redis_handle->del(@chunk, sub { $deleted += $_[0] ? $_[0] : 0 });
        $deleted != @chunk and warn sprintf '%s->mark_item_as_processed: could not remove some meta or item keys', __PACKAGE__;
    }

    $redis_handle->wait_all_responses();

    if (@$failed) {
        warn sprintf '%s->mark_item_as_done: %d/%d items were not removed from working_queue=%s', __PACKAGE__, int(@$failed), int(@$flushed+@$failed), $self->_working_queue;
    }

    # warn Dumper(\%result);

    return \%result;
}

sub unclaim  {
    my $self = shift;
    return $self->__requeue_busy(1, undef, @_);
}

sub requeue_busy {
    my $self = shift;
    return $self->__requeue_busy(0, undef, @_);
}

sub requeue_busy_error {
    my $self = shift;
    my $error= shift;
    return $self->__requeue_busy(0, $error, @_);
}

sub __requeue_busy  {
    my $self  = shift;
    my $place = shift; # 0: producer side, 1: consumer side
    my $error = shift; # error message

    my $items_requeued = 0;
    my $number_of_keys = 2; # we need to specify that the first two arguments refer to specific redis keys
    my $script_name = 'requeue_busy_ng2000';

    eval {
        foreach my $item (@_) {
            $items_requeued += $self->_lua->call(
                $script_name,
                $number_of_keys,
                $self->_working_queue,
                $self->_unprocessed_queue,
                $item->{item_key},
                $place,
                $error,
            );
        }
        1;
    }
    or do {
        my $eval_error = $@ || 'zombie lua error';
        cluck("Lua call went wrong inside $script_name: $eval_error");
    };

    return $items_requeued;
}

# this function returns the oldest item in the queue
sub peek_item {
    my ($self, $subqueue_name) = @_;

    my $subqueue_accessor_name = $VALID_SUBQUEUES{$subqueue_name}
    or die sprintf(q{couldn't find subqueue_accessor for subqueue_name=%s}, $subqueue_name);

    my $subqueue_redis_key;
    unless ( $subqueue_redis_key = $self->$subqueue_accessor_name ) {
        die sprintf "couldn't map subqueue_name=%s to a redis key", $subqueue_name;
    }

    my $redis_handle = $self->redis_handle;

    # take oldest item (and bail if we can't find anything)
    my ($item_key) = $redis_handle->lrange($subqueue_redis_key,-1,-1);
    $item_key or return undef;

    my $item = Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
        item_key => $item_key,
        payload  => $redis_handle->get("item-$item_key") || undef,
        metadata => { $redis_handle->hgetall("meta-$item_key") },
    });

    return $item;
}

sub percent_memory_used {
    my ($self) = @_;

    my $r = $self->redis_handle;

    my (undef, $mem_avail) = $conn->config('get', 'maxmemory');

    if ($mem_avail == 0) {
        warn sprintf "%s->percent_memory_used: maxmemory is set to 0, can't derive a percentage";
        return undef;
    }

    my $info = $r->info('memory');
    my $mem_used = $info->{used_memory};

    return $mem_used == 0 ? 0 : ( $mem_used / $mem_avail ) * 100;
}

1;
