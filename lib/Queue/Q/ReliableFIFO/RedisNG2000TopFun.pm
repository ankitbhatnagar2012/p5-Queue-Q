package Queue::Q::ReliableFIFO::RedisNG2000TopFun;

use strict;
use warnings;

use parent 'Queue::Q::ReliableFIFO';

use Carp qw/croak cluck/;
use Data::UUID::MT;
use Redis qw//;
use Time::HiRes qw//;

use Queue::Q::ReliableFIFO::Lua;
use Queue::Q::ReliableFIFO::ItemNG2000TopFun;

our ( %VALID_SUBQUEUES, %VALID_PARAMS );

BEGIN {
    # define subqueue names and map to accessors
    %VALID_SUBQUEUES = map { $_ => "_${_}_queue" } qw/
        unprocessed
        working
        processed
        failed
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
            _lua
    /],
    setters => {
        set_requeue_limit      => 'requeue_limit',
        set_busy_expiry_time   => 'busy_expiry_time',
        set_claim_wait_timeout => 'claim_wait_timeout'
    }
};
my $UUID = Data::UUID::MT->new( version => '4s' );

####################################################################################################
####################################################################################################

sub new {
    my ($class, %params) = @_;

    foreach my $required_param (qw/server port queue_name/) {
        $params{$required_param}
            or die sprintf(
                q{%s->new(): Missing mandatory parameter "%s".},
                __PACKAGE__, $required_param
            );
    }

    foreach my $provided_param (keys %params) {
        $VALID_PARAMS{$provided_param}
            or die sprintf(
                q{%s->new() encountered an unknown parameter "%s".},
                __PACKAGE__, $provided_param
            );
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
        server    => join(':' => $params{server}, $params{port})
    );

    # populate subqueue attributes with name of redis list
    # e.g. unprocessed -> _unprocessed_queue (accessor name) -> foo_unprocessed (redis list name)
    foreach my $subqueue_name ( keys %VALID_SUBQUEUES ) {
        my $accessor_name = $VALID_SUBQUEUES{$subqueue_name};
        my $redis_list_name = sprintf('%s_%s', $params{queue_name}, $subqueue_name);
        $self->{$accessor_name} = $redis_list_name;
    }

    my %redis_options = %{ $params{redis_options} || {} };

    $self->{redis_handle} //= Redis->new(
        %default_redis_options, %redis_options
    );

    $self->{_lua} = Queue::Q::ReliableFIFO::Lua->new(
        redis_conn => $self->redis_handle
    );

    $params{db_id}
        and $self->redis_handle->select($params{db_id});

    return $self;
}

####################################################################################################
####################################################################################################

sub enqueue_item {
    my $self = shift;

    my $items = ( @_ == 1 and ref $_[0] eq 'ARRAY') ? $_[0] : [ @_ ];

    grep { ref $_ } @$items
        and die sprintf(
            '%s->enqueue_item() encountered a reference. All payloads must be strings!',
            __PACKAGE__
        );

    my $rh = $self->redis_handle;
    my @created;

    foreach my $input_item (@$items) {
        my $item_id  = $UUID->create_hex();
        my $item_key = sprintf('%s-%s', $self->queue_name, $item_id);

        # Create the payload item.
        $rh->setnx("item-$item_key" => $input_item)
            or die sprintf(
                '%s->enqueue_item() failed to setnx() data for item_key=%s. This means the key ' .
                'already existed, which is highly improbable.',
                __PACKAGE__, $item_key
            );

        my $now = Time::HiRes::time;

        my %metadata = (
            process_count => 0, # amount of times a single consumer attempted to handle the item
            bail_count    => 0, # amount of times process_count exceeded its threshold
            time_created  => $now,
            time_enqueued => $now
        );

        # Create metadata. This call will just die if not successful (in the rare event of having
        #   a key with the same name in Redis that doesn't have a hash stored. If it does have one
        #   that indeed has a hash stored (highly unlikely), we are really unfortunate (that is a
        #   silent failure).
        $rh->hmset("meta-$item_key" => %metadata);

        # Enqueue the actual item.
        $rh->lpush($self->_unprocessed_queue, $item_key)
            or die sprintf(
                '%s->enqueue_item() failed to lpush() item_key=%s onto the unprocessed queue (%s).',
                __PACKAGE__, $item_key, $self->_unprocessed_queue
            );

        push @created, Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
            item_key => $item_key,
            payload  => $input_item,
            metadata => \%metadata
        });
    }

    return \@created;
}

####################################################################################################
####################################################################################################

sub claim_item {
    my ($self, $n_items) = @_;
    $self->_claim_item_internal($n_items, BLOCKING);
}

sub claim_item_nonblocking {
    my ($self, $n_items) = @_;
    $self->_claim_item_internal($n_items, NON_BLOCKING);
}

use constant {
    BLOCKING => 0,
    NON_BLOCKING => 1
};

sub _claim_item_internal {
    my ($self, $n_items, $do_blocking) = @_;

    my $timeout           = $self->claim_wait_timeout;
    my $rh                = $self->redis_handle;
    my $unprocessed_queue = $self->_unprocessed_queue;
    my $working_queue     = $self->_working_queue;

    unless ( defined $n_items and $n_items > 0 ) {
        $n_items = 1;
    }

    if ( $n_items == 1 ) {
        if ( $do_blocking == NON_BLOCKING ) {
            my $item_key = $rh->rpoplpush($self->_unprocessed_queue, $self->_working_queue)
                or return;

            $rh->hincrby("meta-$item_key", process_count => 1, sub { });

            my %metadata = $rh->hgetall("meta-$item_key");
            my $payload  = $rh->get("item-$item_key");

            return Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
                item_key => $item_key,
                payload  => $payload,
                metadata => \%metadata
            });
        } else { # $do_blocking == BLOCKING
            my $w_queue = $self->_working_queue;
            my $u_queue = $self->_unprocessed_queue;
            my $item_key = $rh->rpoplpush($u_queue, $w_queue) || $rh->brpoplpush($u_queue, $w_queue, $self->claim_wait_timeout)
                or return;

            $rh->hincrby("meta-$item_key", process_count => 1, sub { });
            my %metadata = $rh->hgetall("meta-$item_key");
            my $payload  = $rh->get("item-$item_key");

            return Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
                item_key => $item_key,
                payload  => $payload,
                metadata => \%metadata
            });
        }
    } else {
        # When fetching multiple items:
        # - Non-blocking mode: We try to fetch one item, and then give up.
        # - Blocking mode: We attempt to fetch the first item using brpoplpush, and if it succeeds,
        #                    we switch to rpoplpush for greater throughput.

        my @items;

        my $handler = sub {
            defined(my $item_key = $_[0])
                or return;

            $rh->hincrby("meta-$item_key", process_count => 1, sub { });
            my %metadata = $rh->hgetall("meta-$item_key");
            my $payload  = $rh->get("item-$item_key");

            keys %metadata
                or warn sprintf(
                    '%s->_claim_item_internal() fetched empty metadata for item_key=%s!',
                    __PACKAGE__, $item_key
                );

            defined $payload
                or warn sprintf(
                    '%s->_claim_item_internal() fetched undefined payload for item_key=%s!',
                    __PACKAGE__, $item_key
                );

            unshift @items, Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
                item_key => $item_key,
                payload  => $payload,
                metadata => \%metadata
            });
        };

        if ($n_items > 30) {
            # Yes, there is a race, but it's an optimization only.
            # This means that after we know how many items we have...
            my ($llen) = $rh->llen($unprocessed_queue);
            $n_items > $llen
                and $n_items = $llen;
        }

        eval {
            $rh->rpoplpush($unprocessed_queue, $working_queue, $handler)
                for 1 .. $n_items;
            $rh->wait_all_responses;

            if ( @items == 0 && $do_blocking ) {
                my $first_item = $rh->brpoplpush($unprocessed_queue, $working_queue, $timeout);

                if (defined $first_item) {
                    $handler->($first_item);

                    $rh->rpoplpush($unprocessed_queue, $working_queue, $handler)
                        for 2 .. $n_items;
                    $rh->wait_all_responses;
                }
            }
            1;
        } or do {
            my $eval_error = $@ || 'zombie error';
            warn sprintf(
                '%s->_claim_item_internal() encountered an exception while claiming bulk items: %s',
                __PACKAGE__, $eval_error
            );
        };

        return @items;
    }

    die sprintf(
        '%s->_claim_item_internal(): Unreachable code. This should never happen.',
        __PACKAGE__
    );
}

####################################################################################################
####################################################################################################

sub mark_item_as_processed {
    my $self = shift;

    my $items = ( @_ == 1 and ref $_[0] eq 'ARRAY' ) ? $_[0] : [ @_ ];

    my $rh = $self->redis_handle;

    my %result = (
        flushed => [],
        failed  => []
    );

    # callback receives result of LREM call for removing item from the working
    # queue and populates %result. if the LREM succeeds, we also need to clean
    # up the payload+metadata.

    foreach my $item (@$items) {
        my $item_key = $item->{item_key};
        my $lrem_direction = 1; # head-to-tail (http://redis.io/commands/lrem)

        $rh->lrem(
            $self->_working_queue,
            $lrem_direction,
            $item_key,
            sub {
                my $result_key = $_[0] ? 'flushed' : 'failed';
                push @{ $result{$result_key} } => $item_key;
            }
        );
    }

    $rh->wait_all_responses;

    my ($flushed, $failed) = @result{qw/flushed failed/};

    my @to_purge = @$flushed;

    while (@to_purge) {
        my @chunk = map  {; ("meta-$_" => "item-$_") }
                    grep { defined $_ }
                    splice @to_purge, 0, 100;

        my $deleted = 0;
        $rh->del(@chunk, sub { $deleted += $_[0] ? $_[0] : 0 });
        $rh->wait_all_responses();
        $deleted == @chunk
            or warn sprintf(
                '%s->mark_item_as_processed() could not remove some item/meta keys!',
                __PACKAGE__
            );
    }

    @$failed
        and warn sprintf(
            '%s->mark_item_as_processed(): %d/%d items were not removed from the working queue (%s)!',
            __PACKAGE__, int(@$failed), int(@$flushed + @$failed), $self->_working_queue
        );

    return \%result;
}

####################################################################################################
####################################################################################################

sub unclaim {
    my $self = shift;

    return $self->__requeue({
        source_queue            => $self->_working_queue,
        items                   => \@_,
        increment_process_count => 0,
        place                   => 1,
        error                   => undef,
    });
}

sub requeue_busy {
    my $self = shift;

    return $self->__requeue({
        source_queue => $self->_working_queue,
        items        => \@_,
        place        => 0,
        error        => undef,
    });
}

sub requeue_busy_error {
    my $self  = shift;
    my $error = shift;

    return $self->__requeue({
        source_queue => $self->_working_queue,
        items        => \@_,
        place        => 0,
        error        => $error,
    });
}

sub requeue_failed_items {
    my $self = shift;
    my $error = shift;

    return $self->__requeue({
        source_queue => $self->_working_queue,
        items        => \@_,
        place        => 1,
        error        => $error,
    });
}

sub __requeue  {
    my ($self, $params) = @_;

    my $place = $params->{place} // 0;
    my $error = $params->{error} // '';
    my $increment_process_count = $params->{increment_process_count} // 1;

    my $source_queue = $params->{source_queue}
        or die sprintf(q{%s->__requeue(): "source_queue" parameter is required.}, __PACKAGE__);

    my $items_requeued = 0;

    eval {
        foreach my $item (@{$params->{items}}) {
            $items_requeued += $self->_lua->call(
                requeue => 3, # Requeue takes 3 keys, the source, ok-destination and fail-destination queues:
                $source_queue, $self->_unprocessed_queue, $self->_failed_queue,
                $item->{item_key},
                $self->requeue_limit,
                $place, # L or R end of distination queue
                $error, # Error string if any
                $increment_process_count
            );
        }
        1;
    } or do {
        my $eval_error = $@ || 'zombie lua error';
        warn sprintf(
            '%s->%s(): Lua call went wrong => %s',
            __PACKAGE__, $internal->{caller}, $eval_error
        );
    };

    return $items_requeued;
}

####################################################################################################
####################################################################################################

sub process_failed_items {
    my ($self, $max_count, $callback) = @_;

    my $temp_table = 'temp-failed-' . $UUID->create_hex(); # Include queue_name too?
    my $rh = $self->redis_handle;

    $rh->renamenx($self->_failed_queue, $temp_table)
        or die sprintf(
            '%s->process_failed_items() failed to renamenx() the failed queue (%s). This means' .
            ' that the key already existed, which is highly improbable.',
            __PACKAGE__, $self->_failed_queue
        );

    my $error_count;
    my @item_keys = $rh->lrange($temp_table, 0, $max_count ? $max_count : -1 );
    my $item_count = @item_keys;

    foreach my $item_key (@item_keys) {
        eval {
            my $item = Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
                item_key => $item_key,
                payload  => $rh->get("item-$item_key") || undef,
                metadata => { $rh->hgetall("meta-$item_key") }
            });

            $callback->($item);
        } or do {
            $error_count++;
        };
    }

    if ($max_count) {
        $rh->ltrim($temp_table, 0, $max_count );
        # move overflow items back to failed queue
        if ($item_count == $max_count) {
            while ( $rh->rpoplpush($temp_table, $self->_failed_queue ) ) {}
        }
    }

    $rh->del($temp_table);

    return ($item_count, $error_count);
}

sub remove_failed_items {
    my ($self, %options) = @_;

    my $min_age  = delete $options{MinAge}       || 0;
    my $min_fc   = delete $options{MinFailCount} || 0;
    my $chunk    = delete $options{Chunk}        || 100;
    my $loglimit = delete $options{LogLimit}     || 100;
    cluck("Invalid option: $_") for (keys %options);

    my $now = Time::HiRes::time;
    my $tc_min = $now - $min_age;

    my $rh = $self->redis_handle;
    my $failed_queue = $self->_failed_queue;

    my @items_removed;

    my ($item_count, $error_count) = $self->process_failed_items($chunk, sub {
        my $item = shift;

        if ($item->{metadata}{process_count} >= $min_fc || $item->{metadata}{time_created} < $tc_min) {
            my $item_key = $item->{item_key};
            $rh->del("item-$item_key");
            $rh->del("meta-$item_key");
            $#items_removed <= $loglimit
                and push @items_removed, $item;
        } else {
            $rh->lpush($failed_queue, $item->{item_key});
        }

        return 1; # Success!
    });

    $error_count
        and warn sprintf(
            '%s->remove_failed_items() encountered %d errors while removing %d failed items!',
            __PACKAGE__, $error_count, $item_count
        );

    return ($item_count, \@items_removed);
}

####################################################################################################
####################################################################################################

sub flush_queue {
    my $self = shift;
    my $rh = $self->redis_handle;
    $rh->multi;
    $rh->del($_) for values %VALID_SUBQUEUES;
    $rh->exec;
    return;
}

####################################################################################################
####################################################################################################

sub queue_length {
    my ($self, $subqueue_name) = @_;

    my $subqueue_accessor_name = $VALID_SUBQUEUES{$subqueue_name}
        or die sprintf(
            q{%s->queue_length() couldn't find subqueue_accessor for subqueue_name=%s.},
            __PACKAGE__, $subqueue_name
        );

    my $subqueue_redis_key = $self->$subqueue_accessor_name;

    my ($llen) = $self->redis_handle->llen($subqueue_redis_key);
    return $llen;
}

####################################################################################################
####################################################################################################

# This function returns the oldest item in the queue, or `undef` if the queue is empty.
sub peek_item {
    my ($self, $subqueue_name) = @_;

    my $subqueue_accessor_name = $VALID_SUBQUEUES{$subqueue_name}
        or die sprintf(
            q{%s->peek_item() couldn't find subqueue_accessor for subqueue_name=%s.},
            __PACKAGE__, $subqueue_name
        );

    my $subqueue_redis_key = $self->$subqueue_accessor_name
        or die sprintf(
            q{%s->peek_item() couldn't map subqueue_name=%s to a Redis key.},
            __PACKAGE__, $subqueue_name
        );

    my $rh = $self->redis_handle;

    # Take the oldest item (and bail out if we can't find anything):
    my @item_key = $rh->lrange($subqueue_redis_key, -1, -1);
    @item_key
        or return undef; # The queue is empty.

    my $item_key = $item_key[0];

    my $payload = $rh->get("item-$item_key");
    defined $payload
        or die sprintf(
            q{%s->peek_item() found item_key=%s but not its payload! This should never happen!!},
            __PACKAGE__, "item-$item_key"
        );

    my @metadata = $rh->hgetall("meta-$item_key");
    @metadata
        or die sprintf(
            q{%s->peek_item() found item_key=%s but not its metadata! This should never happen!!},
            __PACKAGE__, "item-$item_key"
        );

    return Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
        item_key => $item_key,
        payload  => $payload,
        metadata => { @metadata }
    });
}

####################################################################################################
####################################################################################################

sub age {
    my ($self, $subqueue_name) = @_;

    my $subqueue_accessor_name = $VALID_SUBQUEUES{$subqueue_name}
        or die sprintf(
            q{%s->age() couldn't find subqueue_accessor for subqueue_name=%s.},
            __PACKAGE__, $subqueue_name
        );

    my $subqueue_redis_key = $self->$subqueue_accessor_name;
    my $rh = $self->redis_handle;

    # take oldest item
    my ($item_key) = $rh->lrange($subqueue_redis_key,-1,-1);
    $item_key or return undef;

    my $time_created = $rh->hget("meta-$item_key" => 'time_created') || Time::HiRes::time();
    return Time::HiRes::time() - $time_created;
}

####################################################################################################
####################################################################################################

sub percent_memory_used {
    my ($self) = @_;

    my $rh = $self->redis_handle;

    my (undef, $mem_avail) = $rh->config('get', 'maxmemory');

    if ($mem_avail == 0) {
        warn sprintf(
            q{%s->percent_memory_used(): "maxmemory" is set to 0, can't derive a percentage.},
            __PACKAGE__
        );
        return undef;
    }

    return ( $rh->info('memory')->{'used_memory'} / $mem_avail ) * 100;
}

####################################################################################################
####################################################################################################

sub raw_items_unprocessed {
    my $self = shift;
    return $self->_raw_items('unprocessed', @_);
}

sub raw_items_working {
    my $self = shift;
    return $self->_raw_items('working', @_);
}

sub raw_items_failed {
    my $self = shift;
    return $self->_raw_items('failed', @_);
}

sub _raw_items {
    my ($self, $subqueue_name, $n) = @_;

    my $subqueue_redis_key = sprintf('%s_%s', $self->queue_name, $subqueue_name);
    my @item_keys = $self->redis_handle->lrange($subqueue_redis_key, -$n, -1);
}

####################################################################################################
####################################################################################################

sub handle_expired_items {
    my ($self, $timeout, $action) = @_;

    $timeout ||= $self->busy_expiry_time;

    int($timeout)
        or die sprintf(
            q{%s->handle_expired_items(): "$timeout" must be a positive number.},
            __PACKAGE__
        );

    my %valid_actions = map { $_ => 1 } qw/requeue drop/;

    $action && $valid_actions{$action}
        or die sprintf(
            '%s->handle_expired_items(): Unknown action (%s)!',
            __PACKAGE__, $action // 'undefined'
        );

    my $rh = $self->redis_handle;

    my @item_keys = $rh->lrange($self->_working_queue, 0, -1);

    my %item_metadata;

    foreach my $item_key (@item_keys) {
        $rh->hgetall("meta-$item_key" => sub {
            $item_metadata{$item_key} = { @_ } if @_;
        });
    }

    $rh->wait_all_responses;

    my $now = Time::HiRes::time;
    my $window = $now - $timeout;

    my @expired_items;

    for my $item_key (grep { exists $item_metadata{$_} && $item_metadata{$_}{time_enqueued} < $window } @item_keys) {
        my $item = Queue::Q::ReliableFIFO::ItemNG2000TopFun->new(
            item_key => $item_key,
            metadata => $item_metadata{$item_key}
        );

        my $n = $action eq 'requeue'           ?
                    $self->requeue_busy($item) :
                $action eq 'drop'                                   ?
                    $rh->lrem($self->_working_queue, -1, $item_key) :
                    undef;

        $n and push @expired_items, $item;
    }

    return \@expired_items;
}

####################################################################################################
####################################################################################################

sub handle_failed_items {
    my ($self, $action) = @_;

    my %valid_actions = map { $_ => 1 } qw/requeue return/;

    $action && $valid_actions{$action}
        or die sprintf(
            '%s->handle_failed_items(): Unknown action (%s)!',
            __PACKAGE__, $action // 'undefined'
        );

    my $rh = $self->redis_handle;

    my @item_keys = $rh->lrange($self->_failed_queue, 0, -1);

    my ( %item_metadata, @failed_items );

    foreach my $item_key (@item_keys) {
        $rh->hgetall("meta-$item_key" => sub {
            $item_metadata{$item_key} = { @_ };
        });
    }

    $rh->wait_all_responses;

    for my $item_key (@item_keys) {
        my $item = Queue::Q::ReliableFIFO::ItemNG2000TopFun->new(
            item_key => $item_key,
            metadata => $item_metadata{$item_key}
        );

        my $n;
        if ( $action eq 'requeue' ) {
            $n += $self->__requeue(
                source_queue            => $self->_failed_queue,
                items                   => [$item],
                place                   => 0,
                error                   => $item_metadata{$item_key}{last_error},
                # items in failed queue already have process_count=0.
                # this ensures we don't count another attempt:
                increment_process_count => 0
            );
        } elsif ( $action eq 'return' ) {
            $n = $rh->lrem( $self->_failed_queue, -1, $item_key );
        }

        $n and push @failed_items, $item;
    }

    return \@failed_items;
}

####################################################################################################
####################################################################################################

# Legacy method names
{
    no warnings 'once';
    *mark_item_as_done   = \&mark_item_as_processed;
    *requeue_busy_item   = \&requeue_busy;
    *requeue_failed_item = \&requeue_failed_items;
    *memory_usage_perc   = \&percent_memory_used;
    *raw_items_main      = \&raw_items_unprocessed;
    *raw_items_busy      = \&raw_items_working;
}

####################################################################################################
####################################################################################################

1;
