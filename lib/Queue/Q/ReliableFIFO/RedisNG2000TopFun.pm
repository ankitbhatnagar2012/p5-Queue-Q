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

    for my $required_param (qw/server port queue_name/) {
        $params->{$required_param}
            or die sprintf(
                q{%s->new(): Missing mandatory parameter "%s".},
                __PACKAGE__, $required_param
            );
    }

    for my $provided_param (keys %$params) {
        unless ($VALID_PARAMS{$provided_param}) {
            warn sprintf(
                q{%s->new() encountered an unknown parameter "%s".},
                __PACKAGE__, $provided_param
            );
            delete $params->{$provided_param};
        }
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
    for my $subqueue_name ( keys %VALID_SUBQUEUES ) {
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

    for my $input_item (@$items) {
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

sub _claim_items_internal {
    my ($self, $params, $internal) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, $internal->{caller}
        );

    my $n_items = $params->{number_of_items} // 1;
    $n_items =~ m/^\d+$/ && $n_items
        or die sprintf(
            q{%s->%s()'s "number_of_items" parameter has to be a positive integer!},
            __PACKAGE__, $internal->{caller}
        );

    my $mode = $internal->{blocking_mode};

    my $timeout           = $self->claim_wait_timeout;
    my $rh                = $self->redis_handle;
    my $unprocessed_queue = $self->_unprocessed_queue;
    my $working_queue     = $self->_working_queue;

    if ( $n_items == 1 ) {
        if ( $mode == NON_BLOCKING ) {
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
        } else { # $mode == BLOCKING
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
                    '%s->_claim_items_internal() fetched empty metadata for item_key=%s!',
                    __PACKAGE__, $item_key
                );

            defined $payload
                or warn sprintf(
                    '%s->_claim_items_internal() fetched undefined payload for item_key=%s!',
                    __PACKAGE__, $item_key
                );

            unshift @items, Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
                item_key => $item_key,
                payload  => $payload,
                metadata => \%metadata
            });
        };

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

        eval {
            $rh->rpoplpush($unprocessed_queue, $working_queue, $handler)
                for 1 .. $n_items;
            $rh->wait_all_responses;

            if (@items == 0 && $mode == BLOCKING) {
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
                '%s->_claim_items_internal() encountered an exception while claiming bulk items: %s',
                __PACKAGE__, $eval_error
            );
        };

        return @items;
    }

    die sprintf(
        '%s->_claim_items_internal(): Unreachable code. This should never happen.',
        __PACKAGE__
    );
}

sub claim_items {
    my ($self, $params) = @_;
    $self->_claim_items_internal(
        $params,
        { blocking_mode => BLOCKING, caller => 'claim_items' }
    );
}

sub claim_items_nonblocking {
    my ($self, $params) = @_;
    $self->_claim_items_internal(
        $params,
        { blocking_mode => NON_BLOCKING, caller => 'claim_items_nonblocking' }
    );
}

####################################################################################################
####################################################################################################

sub mark_items_as_processed {
    my ($self, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, 'mark_items_as_processed'
        );

    my $items = $params->{items} // [];
    ref $items eq 'ARRAY'
        or die sprintf(
            q{%s->()'s "items" parameter has to be an array!},
            __PACKAGE__, 'mark_items_as_processed'
        );

    @$items
        or die sprintf(
            '%s->%s() expects at least one item to mark as processed!',
            __PACKAGE__, 'mark_items_as_processed'
        );

    grep { not $_->isa('Queue::Q::ReliableFIFO::ItemNG2000TopFun') } @$items
        and die sprintf(
            '%s->%s() only accepts objects of type %s or one of its subclasses.',
            __PACKAGE__, 'mark_items_as_processed', 'Queue::Q::ReliableFIFO::ItemNG2000TopFun'
        );

    my $rh = $self->redis_handle;

    my %result = (
        flushed => [],
        failed  => []
    );

    # The callback receives the result of LREM() for removing the item from the working queue and
    #   populates %result. If LREM() succeeds, we need to clean up the payload and the metadata.
    for my $item (@$items) {
        my $item_key = $item->{item_key};
        my $lrem_direction = 1; # Head-to-tail, though it should not make any difference... since
                                #   the item is supposed to be unique anyway.
                                # See http://redis.io/commands/lrem for details.

        $rh->lrem($self->_working_queue, $lrem_direction, $item_key, sub {
            my $result_key = $_[0] ? 'flushed' : 'failed';
            push @{ $result{$result_key} }, $item_key;
        });
    }

    $rh->wait_all_responses;

    my ($flushed, $failed) = @result{qw/flushed failed/};

    my @to_purge = @$flushed;
    while (@to_purge) {
        my @chunk = map  {; ("meta-$_" => "item-$_") }
                    grep { defined $_ }
                    splice @to_purge, 0, 100;

        my $deleted = 0;
        $rh->del(@chunk, sub { $deleted += $_[0] });
        $rh->wait_all_responses();
        $deleted == @chunk
            or warn sprintf(
                '%s->mark_items_as_processed() could not remove some item/meta keys!',
                __PACKAGE__
            );
    }

    @$failed
        and warn sprintf(
            '%s->mark_items_as_processed(): %d/%d items were not removed from the working queue (%s)!',
            __PACKAGE__, int(@$failed), int(@$flushed + @$failed), $self->_working_queue
        );

    return \%result;
}

####################################################################################################
####################################################################################################

sub unclaim {
    my ($self, $params) = @_;

    return $self->__requeue(
        $params,
        {
            caller                  => 'unclaim',
            increment_process_count => 0,
            place                   => 1,
            source_queue            => $self->_working_queue
        }
    );
}

sub requeue_busy {
    my ($self, $params) = @_;

    return $self->__requeue(
        $params,
        {
            caller                  => 'requeue_busy',
            increment_process_count => 1,
            place                   => 0,
            source_queue            => $self->_working_queue
        }
    );
}

sub requeue_busy_error {
    my ($self, $params) = @_;

    return $self->__requeue(
        $params,
        {
            caller                  => 'requeue_busy_error',
            increment_process_count => 1,
            place                   => 0,
            source_queue            => $self->_working_queue
        }
    );
}

sub requeue_failed_items {
    my ($self, $params) = @_;

    return $self->__requeue(
        $params,
        {
            caller                  => 'requeue_failed_items',
            increment_process_count => 1,
            place                   => 1,
            source_queue            => $self->_failed_queue
        }
    );
}

sub __requeue  {
    my ($self, $params, $internal) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, $internal->{caller}
        );

    my $items = $params->{items} // [];
    ref $items eq 'ARRAY'
        or die sprintf(
            q{%s->%s()'s "items" parameter has to be an array!},
            __PACKAGE__, $internal->{caller}
        );

    @$items
        or die sprintf(
            '%s->%s() expects at least one item!',
            __PACKAGE__, $internal->{caller}
        );

    my $source_queue = $internal->{source_queue}
        or die sprintf(q{%s->__requeue(): "source_queue" parameter is required.}, __PACKAGE__);

    grep { not $_->isa('Queue::Q::ReliableFIFO::ItemNG2000TopFun') } @$items
        and die sprintf(
            '%s->%s() only accepts objects of type %s or one of its subclasses.',
            __PACKAGE__, $internal->{caller}, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun'
        );

    my $place                   = $internal->{place};
    my $error                   = $params->{error} // '';
    my $increment_process_count = $internal->{increment_process_count};

    my $items_requeued = 0;

    eval {
        for my $item (@$items) {
            $items_requeued += $self->_lua->call(
                requeue => 3,
                # Requeue takes 3 keys: The source, ok-destination and fail-destination queues:
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

sub handle_failed_items {
    my ($self, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, 'handle_failed_items'
        );

    my $action = $params->{action} // 'requeue';
    $action eq 'requeue' || $action eq 'drop'
        or die sprintf(
            '%s->handle_failed_items(): Unknown action (%s)!',
            __PACKAGE__, $action
        );

    my $rh = $self->redis_handle;

    my @item_keys = $rh->lrange($self->_failed_queue, 0, -1);

    my (%item_metadata, %item_payload, @failed_items);
    for my $item_key (@item_keys) {
        $rh->get("item-$item_key" => sub {
            defined $_[0]
                or die sprintf(
                    q{%s->%s() found item_key=%s but not its payload! This should never happen!!},
                    __PACKAGE__, 'handle_failed_items', "item-$item_key"
                );

            $item_payload{$item_key} = $_[0];
        });

        $rh->hgetall("meta-$item_key" => sub {
            @_
                or die sprintf(
                    q{%s->%s() found item_key=%s but not its metadata! This should never happen!!},
                    __PACKAGE__, 'handle_failed_items', "meta-$item_key"
                );

            $item_metadata{$item_key} = { @{ $_[0] } }
        });
    }
    $rh->wait_all_responses;

    # Queues (well, lists) in Redis are stored right-to-left.
    for my $item_key (reverse @item_keys) {
        my $item = Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
            item_key => $item_key,
            payload  => $item_payload{$item_key},
            metadata => $item_metadata{$item_key}
        });

        my $n = $action eq 'requeue' ?
                    $self->__requeue(
                        {
                            items => [ $item ],
                            error => $item_metadata{$item_key}->{last_error}
                        },
                        {
                            caller                  => 'handle_failed_items',
                            # Items in the failed queue already have process_count = 0.
                            # This ensures we don't count another attempt:
                            increment_process_count => 0,
                            place                   => 0,
                            source_queue            => $self->_failed_queue
                        }
                    ) :
                $action eq 'drop'                                  ?
                    $rh->lrem($self->_failed_queue, -1, $item_key) :
                    undef;

        $n and push @failed_items, $item;

        if ($action eq 'drop') {
            $rh->del("item-$item_key");
            $rh->del("meta-$item_key");
        }
    }

    return \@failed_items;
}

sub process_failed_items {
    my ($self, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, 'process_failed_items'
        );

    my $max_count = $params->{max_count} // 0;
    $max_count =~ m/^\d+$/ && $max_count
        or die sprintf(
            '%s->process_failed_items(): "max_count" parameter must be a positive integer!',
            __PACKAGE__
        );

    my $callback = $params->{callback}
        or die sprintf(
            '%s->process_failed_items(): "callback" parameter must be provided!',
            __PACKAGE__
        );
    ref $callback eq 'CODE'
        or die sprintf(
            '%s->process_failed_items(): "callback" parameter must be a code reference!',
            __PACKAGE__
        );

    my $temp_table = 'temp-' . $self->_failed_queue . $UUID->create_hex;
    my $rh = $self->redis_handle;

    my $failed_items = $self->queue_length({ subqueue_name => 'failed' });
    $rh->renamenx($self->_failed_queue, $temp_table)
        or die sprintf(
            '%s->process_failed_items() failed to renamenx() the failed queue (%s). This means' .
            ' that the key already existed, which is highly improbable.',
            __PACKAGE__, $self->_failed_queue
        );

    my ($error_count, $i, $item_key, @done);
    my $item_count = $max_count && $max_count < $failed_items ? $max_count : $failed_items;
    my @item_keys  = $rh->lrange($temp_table, -$item_count, -1);

    my $fail = sub {
        $error_count++;
        $rh->lpush($temp_table, shift);
    };
    for ($i = 1; $i <= $item_count; $i++) {
        $item_key = $rh->rpop($temp_table);

        my $payload = $rh->get("item-$item_key");
        defined $payload
            or die sprintf(
                q{%s->peek_item() found item_key=%s but not its payload! This should never happen!!},
                __PACKAGE__, "item-$item_key"
            );

        my @metadata = $rh->hgetall("meta-$item_key")
            or die sprintf(
                q{%s->peek_item() found item_key=%s but not its metadata! This should never happen!!},
                __PACKAGE__, "item-$item_key"
            );

        eval {
            my $response = $callback->(Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
                item_key => $item_key,
                payload  => $payload,
                metadata => { @metadata }
            }));

            if ($response) { # true means success, so we should only declare those items done.
                push @done, $item_key;
            } else {
                $fail->($item_key);
            }
        } or $fail->($item_key);
    }

    # Do we have any overflow items?
    if ($max_count and $failed_items > $max_count) {
        $rh->ltrim($temp_table, 0, $failed_items - $max_count - 1);

        # Move overflow items back to the failed queue.
        while ( $rh->rpoplpush($temp_table, $self->_failed_queue) ) {}
    }

    $rh->del($temp_table, sub {});
    $rh->del("meta-$_", "item-$_", sub {})
        for @done;

    $rh->wait_all_responses;

    return ($item_count, $error_count);
}

sub remove_failed_items {
    my ($self, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, 'remove_failed_items'
        );

    my $min_age = delete($params->{min_age}) // 0;
    $min_age =~ m/^\d+$/
        or die sprintf(
            q{%s->%s(): "%s" must be a non-negative integer.},
            __PACKAGE__, 'remove_failed_items', 'min_age'
        );

    my $min_fc = delete($params->{min_fail_count}) // 0;
    $min_fc =~ m/^\d+$/
        or die sprintf(
            q{%s->%s(): "%s" must be a non-negative integer.},
            __PACKAGE__, 'remove_failed_items', 'min_fail_count'
        );

    my $chunk = delete($params->{chunk}) // 100;
    $chunk =~ m/^\d+$/ && $chunk
        or die sprintf(
            q{%s->%s(): "%s" must be a positive integer.},
            __PACKAGE__, 'remove_failed_items', 'chunk'
        );

    my $log_limit = delete($params->{log_limit}) // 100;
    $log_limit =~ m/^\d+$/ && $log_limit
        or die sprintf(
            q{%s->%s(): "%s" must be a positive integer.},
            __PACKAGE__, 'remove_failed_items', 'log_limit'
        );

    warn sprintf(
        '%s->remove_failed_items(): Invalid options passed => "%s"',
        __PACKAGE__, join('", "', keys %{ $params })
    ) if scalar keys %{ $params };

    my $now = Time::HiRes::time;
    my $min_tc = $now - $min_age;

    my $rh = $self->redis_handle;
    my $failed_queue = $self->_failed_queue;

    my @items_removed;

    my ($item_count, $error_count) = $self->process_failed_items({
        max_count => $chunk,
        callback => sub {
            my $item = shift;

            my $metadata = $item->{metadata};
            if ($metadata->{process_count} >= $min_fc || $metadata->{time_created} < $min_tc) {
                my $item_key = $item->{item_key};
                $rh->del("item-$item_key");
                $rh->del("meta-$item_key");
                @items_removed < $log_limit
                    and push @items_removed, $item;
            } else {
                $rh->lpush($failed_queue, $item->{item_key});
            }

            return 1; # Success!
        }
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
    my ($item_key, $queue_name);

    for my $queue (values %VALID_SUBQUEUES) {
        $queue_name = $self->$queue;
        $rh->lrange($queue_name, 0, -1, sub {
            for $item_key (@{ $_[0] }) {
                $rh->del("item-$item_key", sub {});
                $rh->del("meta-$item_key", sub {});
            }
        });
        $rh->del($queue_name, sub {});
    }

    $rh->wait_all_responses();

    return;
}

####################################################################################################
####################################################################################################

sub queue_length {
    my ($self, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, 'queue_length'
        );

    my $subqueue_name = $params->{subqueue_name} // 'unprocessed';
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

# This function returns the oldest/newest item in the queue, or `undef` if the queue is empty.
sub peek_item {
    my ($self, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, 'peek_item'
        );

    my $direction = $params->{direction} // 'f';
    $direction eq 'b' || $direction eq 'f'
        or die sprintf(
            q{%s->%s(): "direction" is either 'b' (back) or 'f' (front).},
            __PACKAGE__, 'peek_item'
        );
    $direction = $direction eq 'f' ? -1 : 0;

    my $subqueue_name = $params->{subqueue_name} // 'unprocessed';
    my $subqueue_accessor_name = $VALID_SUBQUEUES{$subqueue_name}
        or die sprintf(
            q{%s->%s() couldn't find subqueue_accessor for subqueue_name=%s.},
            __PACKAGE__, 'peek_item', $subqueue_name
        );
    my $subqueue_redis_key = $self->$subqueue_accessor_name
        or die sprintf(
            q{%s->%s() couldn't map subqueue_name=%s to a Redis key.},
            __PACKAGE__, 'peek_item', $subqueue_name
        );

    my $rh = $self->redis_handle;

    # Take the relevant item (and bail out if we can't find anything):
    my @item_key = $rh->lrange($subqueue_redis_key, $direction, $direction);
    @item_key
        or return undef; # The queue is empty.

    my $item_key = $item_key[0];

    my $payload = $rh->get("item-$item_key");
    defined $payload
        or die sprintf(
            q{%s->peek_item() found item_key=%s but not its payload! This should never happen!!},
            __PACKAGE__, "item-$item_key"
        );

    my @metadata = $rh->hgetall("meta-$item_key")
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

sub get_items_age {
    my ($self, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, 'get_item_age'
        );

    my $subqueue_name = $params->{subqueue_name} // 'unprocessed';
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

    my $items = $params->{items} // [];
    ref $items eq 'ARRAY'
        or die sprintf(
            q{%s->get_items_age()'s "items" parameter has to be an array!},
            __PACKAGE__
        );

    if (@$items) {
        grep { not $_->isa('Queue::Q::ReliableFIFO::ItemNG2000TopFun') } @$items
            and die sprintf(
                '%s->get_items_age() only accepts objects of type %s or one of its subclasses.',
                __PACKAGE__, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun'
            );

        my (@ages, $time_created);
        my $rh = $self->redis_handle;
        my $now = Time::HiRes::time();

        for my $item (@$items) {
            $time_created = $item->time_created
                or die sprintf(
                    '%s->get_items_age() found an item without a "time_created" stamp!',
                    __PACKAGE__
                );
            push @ages, ($time_created ? $now - $time_created : 0);
        }

        return \@ages;
    } else { # Return the age of the oldest ('b' for back) or the youngest ('f' for front) item.
             # Return `undef` is the queue is empty.
        my $direction = $params->{direction} // 'f';
        $direction eq 'b' || $direction eq 'f'
            or die sprintf(
                q{%s->get_items_age(): "direction" is either 'b' (back) or 'f' (front).},
                __PACKAGE__
            );
        $direction = $direction eq 'f' ? -1 : 0;

        my $rh = $self->redis_handle;

        # Take the relevant item (and bail out if we can't find anything):
        my @item_key = $rh->lrange($subqueue_redis_key, $direction, $direction);
        @item_key
            or return undef; # The queue is empty.

        my $item_key = $item_key[0];
        my @metadata = $rh->hgetall("meta-$item_key")
            or die sprintf(
                q{%s->%s() found item_key=%s but not its metadata! This should never happen!!},
                __PACKAGE__, 'get_items_age', "item-$item_key"
            );

        my $time_created = $rh->hget("meta-$item_key", 'time_created');
        return defined $time_created ? Time::HiRes::time() - $time_created : 0;
    }
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
    my ($self, $params, $internal) = @_;

    my $n = $params->{number_of_items} || 0;
    $n =~ m/^\d+$/
        or die sprintf(
            q{%s->%s(): "%s" must be a positive integer, or zero to signify all items.},
            __PACKAGE__, $internal->{caller}, 'number_of_items'
        );

    my $rh = $self->redis_handle;
    my $subqueue_redis_key = sprintf('%s_%s', $self->queue_name, $internal->{queue});
    my @item_keys = $rh->lrange($subqueue_redis_key, -$n, -1);

    my %items;
    for my $item_key (@item_keys) {
        $rh->get("item-$item_key", sub {
            defined $_[0]
                or die sprintf(
                    q{%s->%s() found item_key=%s but not its key! This should never happen!!},
                    __PACKAGE__, $internal->{caller}, "item-$item_key"
                );

            $items{$item_key}->{payload} = $_[0];
        });

        $rh->hgetall("meta-$item_key", sub {
            @_
                or die sprintf(
                    q{%s->%s() found item_key=%s but not its metadata! This should never happen!!},
                    __PACKAGE__, $internal->{caller}, "item-$item_key"
                );

            $items{$item_key}->{metadata} = { @{ $_[0] } };
        });
    }

    $rh->wait_all_responses;

    my @items;
    for my $item_key (@item_keys) {
        unshift @items, Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
            item_key => $item_key,
            payload  => $items{$item_key}->{payload},
            metadata => $items{$item_key}->{metadata}
        });
    }
    return \@items;
}

sub raw_items_unprocessed {
    my ($self, $params) = @_;
    $self->_raw_items($params, { queue => 'unprocessed', caller => 'raw_items_unprocessed' });
}

sub raw_items_working {
    my ($self, $params) = @_;
    $self->_raw_items($params, { queue => 'working', caller => 'raw_items_working' });
}

sub raw_items_failed {
    my ($self, $params) = @_;
    $self->_raw_items($params, { queue => 'failed', caller => 'raw_items_failed' });
}

####################################################################################################
####################################################################################################

sub handle_expired_items {
    my ($self, $params) = @_;

    $params //= {};
    ref $params eq 'HASH'
        or die sprintf(
            q{%s->%s() accepts a single parameter (a hash reference) with all named parameters.},
            __PACKAGE__, 'handle_expired_items'
        );

    my $timeout = $params->{timeout} // $self->busy_expiry_time;
    $timeout =~ m/^\d+$/ && $timeout
        or die sprintf(
            q{%s->handle_expired_items(): "$timeout" must be a positive integer!},
            __PACKAGE__
        );

    my $action = $params->{action} // 'requeue';
    $action eq 'requeue' || $action eq 'drop'
        or die sprintf(
            '%s->handle_expired_items(): Unknown action (%s)!',
            __PACKAGE__, $action
        );

    my $rh = $self->redis_handle;
    my (%item_metadata, %item_payload);

    # Either we call Redis to know the length of the working queue, and then call
    #   $rh->lrange($self->_working_queue, -$working_queue_length, -1);
    # which costs us two RPCs, or just reverse what you get from a single RPC...
    my @item_keys = $rh->lrange($self->_working_queue, 0, -1);
    for my $item_key (@item_keys) {
        $rh->get("item-$item_key" => sub {
            defined $_[0]
                or die sprintf(
                    q{%s->%s() found item_key=%s but not its payload! This should never happen!!},
                    __PACKAGE__, 'handle_expired_items', "item-$item_key"
                );

            $item_payload{$item_key} = $_[0];
        });

        $rh->hgetall("meta-$item_key" => sub {
            @_
                or die sprintf(
                    q{%s->%s() found item_key=%s but not its metadata! This should never happen!!},
                    __PACKAGE__, 'handle_expired_items', "meta-$item_key"
                );

            $item_metadata{$item_key} = { @{ $_[0] } }
        });
    }

    $rh->wait_all_responses;

    my $now = Time::HiRes::time;
    my $window = $now - $timeout;

    my @expired_items;
    my @candidates = grep {
        exists $item_metadata{$_} && $item_metadata{$_}->{time_enqueued} < $window
    } @item_keys;

    # ... which is what I've chosen to do.
    for my $item_key (reverse @candidates) {
        my $item = Queue::Q::ReliableFIFO::ItemNG2000TopFun->new({
            item_key => $item_key,
            payload  => $item_payload{$item_key},
            metadata => $item_metadata{$item_key}
        });

        my $n = $action eq 'requeue'                            ?
                    $self->requeue_busy({ items => [ $item ] }) :
                $action eq 'drop'                                   ?
                    $rh->lrem($self->_working_queue, -1, $item_key) :
                    undef;

        $n and push @expired_items, $item;

        if ($action eq 'drop') {
            $rh->del("item-$item_key");
            $rh->del("meta-$item_key");
        }
    }

    return \@expired_items;
}

####################################################################################################
####################################################################################################

# Legacy method names
{
    no warnings 'once';
    *mark_item_as_done   = \&mark_items_as_processed;
    *requeue_busy_item   = \&requeue_busy;
    *requeue_failed_item = \&requeue_failed_items;
    *memory_usage_perc   = \&percent_memory_used;
    *raw_items_main      = \&raw_items_unprocessed;
    *raw_items_busy      = \&raw_items_working;
}

####################################################################################################
####################################################################################################

1;
