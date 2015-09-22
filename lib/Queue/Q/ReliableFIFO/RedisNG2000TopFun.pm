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
        values %VALID_SUBQUEUES,
        keys %VALID_PARAMS,
        '_lua'
    ],
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
    my ($class, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->new() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__
        );

    foreach my $required_param (qw/server port queue_name/) {
        $params->{$required_param}
            or die sprintf(
                q{%s->new(): Missing mandatory parameter "%s".},
                __PACKAGE__, $required_param
            );
    }

    foreach my $provided_param (keys %$params) {
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
        %$params
    } => $class);

    my %default_redis_options = (
        reconnect => 60,
        encoding  => undef, # force undef for binary data
        server    => join(':' => $params->{server}, $params->{port})
    );

    # populate subqueue attributes with name of redis list
    # e.g. unprocessed -> _unprocessed_queue (accessor name) -> foo_unprocessed (redis list name)
    foreach my $subqueue_name ( keys %VALID_SUBQUEUES ) {
        my $accessor_name = $VALID_SUBQUEUES{$subqueue_name};
        my $redis_list_name = sprintf('%s_%s', $params->{queue_name}, $subqueue_name);
        $self->{$accessor_name} = $redis_list_name;
    }

    my %redis_options = %{ $params->{redis_options} || {} };

    $self->{redis_handle} //= Redis->new(
        %default_redis_options, %redis_options
    );

    $self->{_lua} = Queue::Q::ReliableFIFO::Lua->new(
        redis_conn => $self->redis_handle
    );

    $params->{db_id}
        and $self->redis_handle->select($params->{db_id});

    return $self;
}

####################################################################################################
####################################################################################################

sub enqueue_items {
    my ($self, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->enqueue_items() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__
        );

    my $items = $params->{items} // [];
    ref $items eq 'ARRAY'
        or die sprintf(
            q{%s->enqueue_items()'s "items" parameter has to be an array!},
            __PACKAGE__
        );

    @$items
        or die sprintf(
            '%s->enqueue_items() expects at least one item to enqueue!',
            __PACKAGE__
        );

    grep { ref $_ || not defined $_ } @$items
        and die sprintf(
            '%s->enqueue_items(): All payloads must be strings!',
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
                '%s->enqueue_items() failed to setnx() data for item_key=%s. This means the key ' .
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
                '%s->enqueue_items() failed to lpush() item_key=%s onto the unprocessed queue (%s).',
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

use constant {
    BLOCKING => 0,
    NON_BLOCKING => 1
};

sub _claim_item_internal {
    my ($self, $n_items, $do_blocking) = @_;

    if (defined $n_items) {
        $n_items =~ m/^\d+$/ && $n_items
            or die sprintf(
                '%s->claim_item(): The number of items must be a positive integer!',
                __PACKAGE__
            );
    } else {
        $n_items = 1;
    }

    my $timeout           = $self->claim_wait_timeout;
    my $rh                = $self->redis_handle;
    my $unprocessed_queue = $self->_unprocessed_queue;
    my $working_queue     = $self->_working_queue;

    defined $n_items && $n_items > 0
        or $n_items = 1;

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
            my $llen = $rh->llen($unprocessed_queue);
            # ... we only take those. But the point is that, between these two comments (err, maybe
            #   the code statements are more important) there might have been an enqueue_items(), so
            #   that we are actually grabbing less than what the user asked for. But we are OK with
            #   that, since the new items are too fresh anyway (they might even be too hot for that
            #   user's tongue).
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

sub claim_item {
    my ($self, $n_items) = @_;
    $self->_claim_item_internal($n_items, BLOCKING);
}

sub claim_item_nonblocking {
    my ($self, $n_items) = @_;
    $self->_claim_item_internal($n_items, NON_BLOCKING);
}

####################################################################################################
####################################################################################################

sub mark_item_as_processed {
    my $self = shift;

    my $items = ( @_ == 1 and ref $_[0] eq 'ARRAY' ) ? $_[0] : [ @_ ];

    @$items
        or die sprintf(
            '%s->mark_item_as_processed() expects at least one item to mark as processed!',
            __PACKAGE__
        );

    grep { not $_->isa('Queue::Q::ReliableFIFO::ItemNG2000TopFun') } @{ $items }
        and die sprintf(
            '%s->mark_item_as_processed() only accepts objects of type %s or one of its subclasses.',
            __PACKAGE__, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun'
        );

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

    return $self->__requeue(
        {
            caller                  => 'unclaim',
            error                   => undef,
            increment_process_count => 0,
            place                   => 1,
            source_queue            => $self->_working_queue
        },
        @_
    );
}

sub requeue_busy {
    my $self = shift;

    return $self->__requeue(
        {
            caller       => 'requeue_busy',
            error        => '',
            place        => 0,
            source_queue => $self->_working_queue
        },
        @_
    );
}

sub requeue_busy_error {
    my $self  = shift;
    my $error = shift;

    return $self->__requeue(
        {
            caller       => 'requeue_busy_error',
            error        => $error,
            place        => 0,
            source_queue => $self->_working_queue,
        },
        @_
    );
}

sub requeue_failed_items {
    my $self = shift;
    my $error = shift;

    return $self->__requeue(
        {
            caller       => 'requeue_failed_items',
            error        => $error,
            place        => 1,
            source_queue => $self->_working_queue
        },
        @_
    );
}

sub __requeue  {
    my $self = shift;
    my $params = shift;

    my $items = ( @_ == 1 and ref $_[0] eq 'ARRAY' ) ? $_[0] : [ @_ ];

    my $source_queue = $params->{source_queue}
        or die sprintf(q{%s->__requeue(): "source_queue" parameter is required.}, __PACKAGE__);

    grep { not $_->isa('Queue::Q::ReliableFIFO::ItemNG2000TopFun') } @{ $items }
        and die sprintf(
            '%s->%s() only accepts objects of type %s or one of its subclasses.',
            __PACKAGE__, $params->{caller}, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun'
        );

    my $place = $params->{place} // 0;
    my $error = $params->{error} // '';
    my $increment_process_count = $params->{increment_process_count} // 1;

    my $items_requeued = 0;

    eval {
        foreach my $item (@$items) {
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

    defined $max_count && $max_count !~ m/^\d+$/
        and die sprintf(
            '%s->process_failed_items(): "$max_count" must be a positive integer!',
            __PACKAGE__
        );

    ref $callback eq 'CODE'
        or die sprintf(
            '%s->process_failed_items(): "$callback" must be a code reference!',
            __PACKAGE__
        );

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
    cluck(__PACKAGE__ . qq{->remove_failed_items(): Invalid option "$_"!})
        for keys %options;

    my $now = Time::HiRes::time;
    my $tc_min = $now - $min_age;

    my $rh = $self->redis_handle;
    my $failed_queue = $self->_failed_queue;

    my @items_removed;

    my ($item_count, $error_count) = $self->process_failed_items($chunk, sub {
        my $item = shift;

        my $metadata = $item->{metadata};
        if ($metadata->{process_count} >= $min_fc || $metadata->{time_created} < $tc_min) {
            my $item_key = $item->{item_key};
            $rh->del("item-$item_key");
            $rh->del("meta-$item_key");
            @items_removed < $loglimit
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

    my $subqueue_redis_key = $self->$subqueue_accessor_name
        or die sprintf(
            q{%s->queue_length() couldn't map subqueue_name=%s to a Redis key.},
            __PACKAGE__, $subqueue_name
        );

    return $self->redis_handle->llen($subqueue_redis_key);
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

sub get_item_age {
    my ($self, $subqueue_name) = @_;

    my $subqueue_accessor_name = $VALID_SUBQUEUES{$subqueue_name}
        or die sprintf(
            q{%s->get_item_age() couldn't find subqueue_accessor for subqueue_name=%s.},
            __PACKAGE__, $subqueue_name
        );

    my $subqueue_redis_key = $self->$subqueue_accessor_name
        or die sprintf(
            q{%s->get_item_age() couldn't map subqueue_name=%s to a Redis key.},
            __PACKAGE__, $subqueue_name
        );

    my $rh = $self->redis_handle;

    # Take the oldest item (and bail out if we can't find anything):
    my @item_key = $rh->lrange($subqueue_redis_key, -1, -1);
    @item_key
        or return undef; # The queue is empty.

    my $time_created = $rh->hget("meta-$item_key[0]", 'time_created');
    return defined $time_created ? Time::HiRes::time() - $time_created : 0;
}

####################################################################################################
####################################################################################################

sub percent_memory_used {
    my $self = shift;

    my $rh = $self->redis_handle;

    my (undef, $mem_avail) = $rh->config_get('maxmemory');
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

sub _raw_items {
    my ($self, $subqueue_name, $n) = @_;

    my $subqueue_redis_key = sprintf('%s_%s', $self->queue_name, $subqueue_name);
    my @item_keys = $self->redis_handle->lrange($subqueue_redis_key, -$n, -1);
}

sub raw_items_unprocessed {
    shift->_raw_items('unprocessed', @_);
}

sub raw_items_working {
    shift->_raw_items('working', @_);
}

sub raw_items_failed {
    shift->_raw_items('failed', @_);
}

####################################################################################################
####################################################################################################

sub handle_expired_items {
    my ($self, $timeout, $action) = @_;

    $timeout ||= $self->busy_expiry_time;

    int($timeout)
        or die sprintf(
            q{%s->handle_expired_items(): "$timeout" must be a positive integer!},
            __PACKAGE__
        );

    $action && ( $action eq 'requeue' || $action eq 'drop' )
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

    $action && ( $action eq 'requeue' || $action eq 'return' )
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
                {
                    error                   => $item_metadata{$item_key}{last_error},
                    # Items in the failed queue already have process_count = 0.
                    # This ensures we don't count another attempt:
                    increment_process_count => 0,
                    place                   => 0,
                    source_queue            => $self->_failed_queue
                },
                $item
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
