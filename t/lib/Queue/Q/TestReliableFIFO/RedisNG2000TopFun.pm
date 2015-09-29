package # Hide from PAUSE
    Queue::Q::TestReliableFIFO::RedisNG2000TopFun;

use strict;
use warnings;

use Test::More;
use Test::Fatal qw/dies_ok lives_ok/;

my ($q, $unprocessed, $processed, $in_progress, $failed);

sub report {
    my ($note, $unprocessed_q, $working_q, $failed_q, $processed_q) = @_;

    diag("\n$note:\n") if $note;

    $unprocessed_q and is(
        $q->queue_length({ subqueue_name => 'unprocessed' }),
        $unprocessed,
        "$unprocessed elements are currently in the queue."
    );
    $working_q and is(
        $q->queue_length({ subqueue_name => 'working' }),
        $in_progress,
        "$in_progress elements are currently being processed."
    );
    $failed_q and is(
        $q->queue_length({ subqueue_name => 'failed' }),
        $failed,
        "$failed elements are currently in the failed queue."
    );
    $processed_q and is(
        $q->queue_length({ subqueue_name => 'processed' }),
        $processed,
        "$processed elements have already been processed."
    );

    $unprocessed_q and is_deeply(
        [ map { $_->data } @{ $q->raw_items_unprocessed } ],
        $unprocessed_q,
        'Unprocessed queue contents: ["' . join('", "', @$unprocessed_q) . '"]'
    );
    $working_q and is_deeply(
        [ map { $_->data } @{ $q->raw_items_working } ],
        $working_q,
        'Working queue contents: ["' . join('", "', @$working_q) . '"]'
    );
    $failed_q and is_deeply(
        [ map { $_->data } @{ $q->raw_items_failed } ],
        $failed_q,
        'Failed queue contents: ["' . join('", "', @$failed_q) . '"]'
    );
    $processed_q and is_deeply(
        [ map { $_->data } @{ $q->raw_items_processed } ],
        $processed_q,
        'Processed queue contents: ["' . join('", "', @$processed_q) . '"]'
    );
}

my $ITEM = 'Queue::Q::ReliableFIFO::ItemNG2000TopFun';

sub test_reliable_fifo {
    $q = shift;

    my (@elements, $items_in, $items_out);

    ok(defined $q->$_, qq{"$_" is a mandatory parameter!})
        for qw/server port queue_name/;

    for (keys %Queue::Q::ReliableFIFO::RedisNG2000TopFun::VALID_SUBQUEUES) {
        $items_in = "_${_}_queue";
        is($q->$items_in, $q->queue_name . "_$_", 'Class::XSAccessor methods are working properly.');
    }

    dies_ok { $q->enqueue_items(                      ) } 'You have to enqueue something!';
    dies_ok { $q->enqueue_items({                    }) } 'You have to enqueue something!';
    dies_ok { $q->enqueue_items({ items => undef     }) } 'You have to enqueue something!';
    dies_ok { $q->enqueue_items({ items => []        }) } 'You have to enqueue something!';
    dies_ok { $q->enqueue_items({ items => [ undef ] }) } 'You can queue strings only. No undefs.';
    dies_ok { $q->enqueue_items({ items => [ []    ] }) } 'You can queue strings only. Nothing else.';
    dies_ok { $q->enqueue_items({ items => [ {}    ] }) } 'You can queue strings only. Nothing else.';

    # Clean up so the tests can make sense...
    $q->flush_queue;
    $unprocessed = $in_progress = $failed = $processed = 0;
    report('The queue is initially empty', [], []);

    $q->enqueue_items({ items => [ 1, 2 ] });
    $unprocessed += 2;
    report('Two items enqueued as a list', [ 1, 2 ], []);

    $q->enqueue_items({ items => [ 3, 4 ] });
    $unprocessed += 2;
    report('Two other items enqueued as an arrayref', [ 1, 2, 3, 4 ], []);

    $items_in = $q->raw_items_unprocessed();
    is(ref $items_in, 'ARRAY', 'raw_items_unprocessed() returns an arrayref.');
    is(@$items_in, $unprocessed, 'raw_items_unprocessed() returns the correct number of elements.');
    is(ref $items_in->[0], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[0]->data, 1, 'raw_items_unprocessed() returns data in the correct order.');
    is(ref $items_in->[1], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[1]->data, 2, 'raw_items_unprocessed() returns data in the correct order.');
    is(ref $items_in->[2], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[2]->data, 3, 'raw_items_unprocessed() returns data in the correct order.');
    is(ref $items_in->[3], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[3]->data, 4, 'raw_items_unprocessed() returns data in the correct order.');

    $items_in = $q->raw_items_unprocessed({ number_of_items => 0 }); # Zero means all the elements.
    is(ref $items_in, 'ARRAY', 'raw_items_unprocessed() returns an arrayref.');
    is(@$items_in, $unprocessed, 'raw_items_unprocessed() returns the correct number of elements.');
    is(ref $items_in->[0], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[0]->data, 1, 'raw_items_unprocessed() returns data in the correct order.');
    is(ref $items_in->[1], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[1]->data, 2, 'raw_items_unprocessed() returns data in the correct order.');
    is(ref $items_in->[2], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[2]->data, 3, 'raw_items_unprocessed() returns data in the correct order.');
    is(ref $items_in->[3], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[3]->data, 4, 'raw_items_unprocessed() returns data in the correct order.');

    $items_in = $q->raw_items_unprocessed({ number_of_items => 2 });
    is(ref $items_in, 'ARRAY', 'raw_items_unprocessed() returns an arrayref.');
    is(@$items_in, 2, 'raw_items_unprocessed() returns the correct number of elements.');
    is(ref $items_in->[0], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[0]->data, 1, 'raw_items_unprocessed() returns data in the correct order.');
    is(ref $items_in->[1], $ITEM, 'raw_items_unprocessed() returns the correct type of objects.');
    is($items_in->[1]->data, 2, 'raw_items_unprocessed() returns data in the correct order.');

    dies_ok {  
        $q->peek_item({ direction => 'f', subqueue_name => 'unprocess' })->data
    } 'peek_item() correctly dies if called with a wrong "subqueue_name".';
    dies_ok {  
        $q->peek_item({ direction => 'u', subqueue_name => 'unprocess' })->data
    } 'peek_item() correctly dies if called with a wrong "direction".';

    is(
        $q->peek_item({ direction => 'f', subqueue_name => 'unprocessed' })->data,
        1,
        'peek_item() sees the correct side.'
    );
    is(
        $q->peek_item({ direction => 'b', subqueue_name => 'unprocessed' })->data,
        4,
        'peek_item() sees the correct side.'
    );

    $q->flush_queue;
    $unprocessed = $in_progress = $failed = $processed = 0;
    report('Flushed queue is empty', [], []);

    $q->enqueue_items({ items => [ 1, 2 ] });
    $unprocessed += 2;
    report('Two items enqueued as a list', [ 1, 2 ], []);

    $q->enqueue_items({ items => [ 3, 4 ] });
    $unprocessed += 2;
    report('Two other items enqueued as an arrayref', [ 1, 2, 3, 4 ], []);

    $items_in = @elements = ( 15 .. 25 );
    $unprocessed += $items_in;
    $items_out = $q->enqueue_items({ items => \@elements });
    report("After enqueuing $items_in items", [ 1, 2, 3, 4, 15 .. 25 ], []);
    is($items_in, scalar(@{ $items_out }), "$items_in items enqueued.");

    dies_ok {
        $q->claim_items({ number_of_items => -2 })
    } 'claim_items() dies correctly when we ask for something other than a positive integer.';

    my $item = $q->claim_items();
    $unprocessed--; $in_progress++;
    isa_ok($item, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun');
    is($item->data, 1, 'The data of the first item is correct (using claim_items()).');
    report('After claiming the first item', [ 2, 3, 4, 15 .. 25 ], [ 1 ]);

    $q->mark_items_as_processed({ items => [ $item ] });
    $in_progress--; $processed++;
    report('After processing the first item', [ 2, 3, 4, 15 .. 25 ], []);

    $item = $q->claim_items_nonblocking();
    $unprocessed--; $in_progress++;
    isa_ok($item, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun');
    is($item->data, 2, 'The data of the second item is correct (using claim_items_nonblocking()).');
    report('After claiming the second item', [ 3, 4, 15 .. 25 ], [ 2 ]);

    @elements = $q->claim_items({ number_of_items => $items_out = 2 });
    $unprocessed -= $items_out; $in_progress += $items_out;
    is(scalar(@elements), $items_out, 'Claiming two items in one go with claim_items().');
    is($elements[0]->data, 3, 'The data of the third item is correct.');
    is($elements[1]->data, 4, 'The data of the fourth item is correct.');
    report('After claiming the third and the fourth items', [ 15 .. 25 ], [ 2, 3, 4 ]);

    $q->mark_items_as_processed({ items => [ $elements[0] ] });
    $in_progress--; $processed++; shift @elements;
    report('After processing the third item', [ 15 .. 25 ], [ 2, 4 ]);
    $q->mark_items_as_processed({ items => [ $item ] });
    $in_progress--; $processed++;
    report('After processing the second item', [ 15 .. 25 ], [ 4 ]);

    push @elements, $q->claim_items_nonblocking({ number_of_items => $items_out = 3 });
    $unprocessed -= $items_out; $in_progress += $items_out;
    is(scalar(@elements), $in_progress, 'Claiming a lot of items non-blockingly via claim_items_nonblocking().');
    isa_ok($_, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun') for @elements;
    report("After claiming $items_out items", [ 18 .. 25 ], [ 4, 15 .. 17 ]);

    $q->enqueue_items({ items => [ 'f' ] });
    $unprocessed++;
    report('After queuing a string', [ 18 .. 25, 'f' ], [ 4, 15 .. 17 ]);

    push @elements, $q->claim_items({ number_of_items => 20 }); # More than what we have...
    $unprocessed = 0; $in_progress += 9;
    is(scalar(@elements), $in_progress);
    isa_ok($_, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun') for @elements[-9 .. -1];
    is_deeply(
        [ map $_->data, @elements[-9 .. -1] ],
        [ 18 .. 25, 'f'],
        'Claiming items via claim_items().'
    );

    $unprocessed = grep { not defined $_ } @elements;
    ok(
        0 == $unprocessed,
        'Claimed items are never undefined, even if we ask for too many elements.'
    );
    report('After claiming more than we have in the queue', [], [ 4, 15 .. 25, 'f' ]);

    $item = $q->claim_items();
    ok(!defined $item, 'Claiming an item blockingly via claim_items() when the queue is empty. This test is expected to take some time, since the call will block for claim_wait_timeout() seconds.');
    report('Claiming one item blockingly via claim_items() when the queue is empty', [], [ 4, 15 .. 25, 'f' ]);

    $item = $q->claim_items_nonblocking();
    ok(!defined $item, 'This test, on the contrary, is expected to be instantaneous, since the call will not block.');
    report('Claiming one item non-blockingly via claim_items_nonblocking() when the queue is empty', [], [ 4, 15 .. 25, 'f' ]);

    push @elements, $q->claim_items({ number_of_items => 5 });
    is(scalar(@elements), $in_progress, 'Claiming a lot of items blockingly via claim_items(). This test is expected to take some time, since the call will block for claim_wait_timeout() seconds.');
    report('Claiming a lot of items blockingly via claim_items() when the queue is empty', [], [ 4, 15 .. 25, 'f' ]);

    push @elements, $q->claim_items_nonblocking({ number_of_items => 1234 }); # A LOT!
    is(scalar(@elements), $in_progress, 'Claiming a lot of items non-blockingly via claim_items_nonblocking().');
    report('Claiming a lot of items non-blockingly via claim_items_nonblocking() when the queue is empty', [], [ 4, 15 .. 25, 'f' ]);

    # Grepping for defined is just an extra check that we never have undefs returned from claim_items().
    $q->mark_items_as_processed({ items => [ grep defined, @elements[4 .. $#elements] ] });
    $processed += (@elements - 4);
    $in_progress -= (@elements - 4);
    report('After processing all but four of the elements', [], [ 4, 15 .. 17 ]);

    dies_ok {
        $q->mark_items_as_processed()
    } 'mark_items_as_processed() correctly dies if called with no arguments.';
    dies_ok {
        $q->mark_items_as_processed(undef)
    } 'mark_items_as_processed() correctly dies if called with no arguments.';
    dies_ok {
        $q->mark_items_as_processed({ items => [] })
    } 'mark_items_as_processed() correctly dies if called with no items!';
    dies_ok {
        $q->mark_items_as_processed({ items => [ undef ] })
    } 'mark_items_as_processed() correctly dies if called with undefined items.';
    dies_ok {
        $q->mark_items_as_processed([ items => [ undef ] ])
    } 'mark_items_as_processed() correctly dies if called with something other than a hash reference.';

    $q->mark_items_as_processed({ items => [ $_ ] })
        for ($elements[3], $elements[2], $elements[1], $elements[0]);
    $in_progress = 0;
    $processed += 4;
    report('After processing the last four elements in reverse order', [], []);

    my ($element0, $element1, $element2);
    lives_ok {
        $element0 = $q->enqueue_items({ items => [ '' ] })->[0];
        $unprocessed++;
    } 'You can queue strings only, including an empty string.';
    report('After enqueuing an item', [ '' ], []);

    $element1 = $q->enqueue_items({ items => [ 1 ] })->[0];
    $unprocessed++;
    report('After enqueuing a second item', [ '', 1 ], []);

    $element2 = $q->enqueue_items({ items => [ 2 ] })->[0];
    $unprocessed++;
    report('After enqueuing a third item', [ '', 1, 2 ], []);

    push @elements, $q->claim_items_nonblocking({ number_of_items => 2 });
    $unprocessed -= 2; $in_progress += 2;
    report('After claiming two of the three remaining elements', [ 2 ], [ '', 1 ]);

    # Another way of doing the last test "Working queue contents"...
    is($elements[-2]->data, $element0->data, 'Claiming with claim_items_nonblocking() happens in the correct order...');
    is($elements[-1]->data, $element1->data, 'Claiming with claim_items_nonblocking() happens in the correct order...');

    dies_ok { $q->unclaim(undef)              } 'unclaim() correctly dies if called with no arguments.';
    dies_ok { $q->unclaim()                   } 'unclaim() correctly dies if called with undefined arguments.';
    dies_ok { $q->requeue_busy(undef)         } 'requeue_busy() correctly dies if called with no arguments.';
    dies_ok { $q->requeue_busy()              } 'requeue_busy() correctly dies if called with undefined arguments.';
    dies_ok { $q->requeue_busy_error(undef)   } 'requeue_busy_error() correctly dies if called with no arguments.';
    dies_ok { $q->requeue_busy_error()        } 'requeue_busy_error() correctly dies if called with undefined arguments.';
    dies_ok { $q->requeue_failed_items(undef) } 'requeue_failed_items() correctly dies if called with no arguments.';
    dies_ok { $q->requeue_failed_items()      } 'requeue_failed_items() correctly dies if called with undefined arguments.';

    # Put element1 at the beginning of the unprocessed queue.
    $q->unclaim({ items => [ $element1 ] });
    $unprocessed++; $in_progress--;
    report('After unclaiming one of the three remaining elements (out of order, to test that as well)', [ 1, 2 ], [ '' ]);
    is(
        $q->peek_item({ direction => 'f', subqueue_name => 'unprocessed' })->data,
        $element1->data,
        'unclaim() pushes data back in the correct order.'
    );

    # Put element0 at the beginning of the unprocessed queue, ahead of element1.
    $q->unclaim({ items => [ $element0 ] });
    $unprocessed++; $in_progress--;
    report('After unclaiming the remaining element', [ '', 1, 2 ], []);
    is(
        $q->peek_item({ direction => 'f', subqueue_name => 'unprocessed' })->data,
        $element0->data,
        'unclaim() pushes data back in the correct order.'
    );

    push @elements, $q->claim_items_nonblocking({ number_of_items => 2 });
    $unprocessed -= 2; $in_progress += 2;
    report('After claiming two elements out of the remaining three', [ 2 ], [ '', 1 ]);
    # So, element0 has to come first!
    is(
        $elements[-2]->data,
        $element0->data,
        'Claiming with claim_items_nonblocking() happens in the correct order...'
    );
    is(
        $elements[-1]->data,
        $element1->data,
        'Claiming with claim_items_nonblocking() happens in the correct order...'
    );

    push @elements, $q->claim_items();
    $unprocessed--; $in_progress++;
    report('After claiming the last element', [], [ '', 1, 2 ]);
    is(
        $elements[-1]->data,
        $element2->data,
        'Claiming with claim_items() happens in the correct order...'
    );

    $q->requeue_busy({ items => [ $element2 ] });
    $unprocessed++; $in_progress--;
    report('After requeuing an element', [ 2 ], [ '', 1 ]);

    $q->requeue_busy({ items => [ $element1 ] });
    $unprocessed++; $in_progress--;
    report('After requeuing another element', [ 2, 1 ], [ '' ]);
    is(
        $q->peek_item({ direction => 'b', subqueue_name => 'unprocessed' })->data,
        $element1->data,
        'requeue_busy() works in the correct order.'
    );

    # Cleaning-up... getting so many back to guarantee that the queue is empty even if the test were modified.
    push @elements, $q->claim_items_nonblocking({ number_of_items => 123 });
    $unprocessed -= 2;
    $in_progress += 2;
    report('After evacuating the unprocessed queue', [], [ '', 2, 1 ]);

    my $sleep_time = $q->busy_expiry_time + 1;
    sleep($sleep_time); # We want the items to expire.

    # While we are here...
    my $age = $q->get_items_age({ direction => 'f', subqueue_name => 'working' });
    cmp_ok($age, '>=', $sleep_time, q{Testing get_items_age() on the queue's front.});
    $age = $q->get_items_age({ direction => 'b', subqueue_name => 'working' });
    cmp_ok($age, '>=', $sleep_time, q{Testing get_items_age() on the queue's back.});
    ok(
        !defined $q->get_items_age({ direction => 'b' }),
        'get_items_age() returns `undef` when the queue is empty.'
    );
    $age = $q->get_items_age({ items => \@elements });
    cmp_ok($_, '>=', $sleep_time, q{Testing get_items_age() when you pass items to it.})
        for @$age;

    my $expired_items = $q->handle_expired_items({ action => 'requeue' });
    $unprocessed += $in_progress;
    $in_progress = 0;
    report('After handling expired items via requeuing', [ '', 2, 1 ], []);
    is(@$expired_items, $unprocessed, 'All items have expired and have been sent back to the unprocessed queue.');
    isa_ok($_, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun')
        for @$expired_items;

    push @elements, $q->claim_items({ number_of_items => 3 });
    $unprocessed -= 3;
    $in_progress += 3;
    report('After evacuating the unprocessed queue', [], [ '', 2, 1 ]);

    sleep($q->busy_expiry_time + 1); # We want the items to expire.

    $expired_items = $q->handle_expired_items({ action => 'drop' });
    $in_progress = 0;
    report('After handling expired items via requeuing', [], []);
    is(@$expired_items, 3, 'All items have expired and have been sent back to the unprocessed queue.');
    isa_ok($_, 'Queue::Q::ReliableFIFO::ItemNG2000TopFun')
        for @$expired_items;

    # Testing Failures.
    $element1 = $q->enqueue_items({ items => [ 1 ] })->[0];
    $unprocessed++;
    report('After enqueuing an item', [ 1 ], []);

    $element2 = $q->enqueue_items({ items => [ 2 ] })->[0];
    $unprocessed++;
    report('After enqueuing a second item', [ 1, 2 ], []);

    $element0 = $q->enqueue_items({ items => [ 0 ] })->[0];
    $unprocessed++;
    report('After enqueuing a third item', [ 1, 2, 0 ], []);

    # Illegally :-D simulating failure...
    $q->redis_handle->rpoplpush($q->_unprocessed_queue, $q->_failed_queue)
        for 1 .. 3;
    $unprocessed -= 3; $failed += 3;
    report('After simulating failure and evacuating the unprocessed queue', [], [], [ 1, 2, 0 ]);

    $items_in = $q->handle_failed_items({ action => 'requeue' });
    $unprocessed += 3; $failed -= 3;
    report('After requeuing all the failed items to the unprocessed queue', [ 1, 2, 0 ], [], []);

    $q->redis_handle->rpoplpush($q->_unprocessed_queue, $q->_failed_queue)
        for 1 .. 3;
    $unprocessed -= 3; $failed += 3;
    report('After simulating failure and evacuating the unprocessed queue', [], [], [ 1, 2, 0 ]);

    $items_in = $q->handle_failed_items({ action => 'drop' });
    $failed -= 3;
    report('After dropping all the failed items', [], [], []);

    $items_in = $q->enqueue_items({ items => [ 1, 2, 0, 4, 5, 6, 7, 8 ] });
    $unprocessed += 8;
    report('After enqueuing 8 items in one go', [ 1, 2, 0, 4, 5, 6, 7, 8 ], []);

    $q->redis_handle->rpoplpush($q->_unprocessed_queue, $q->_failed_queue)
        for 1 .. 6;
    $unprocessed -= 6; $failed += 6;
    report('After simulating failure for 6 items out of 8', [ 7, 8 ], [], [ 1, 2, 0, 4, 5, 6 ]);

    my @failed = ();
    $q->process_failed_items({ max_count => 3, callback => sub { push @failed, shift->data } });
    $failed -= 3;
    report('After process_failed_items()', [ 7, 8 ], [], [ 4, 5, 6 ]);
    is_deeply(\@failed, [ 1, 2, 0 ], 'process_failed_items() works in the correct order.');

    # Testing successes.
    $element1 = $q->enqueue_items({ items => [ 1 ] })->[0];
    $unprocessed++;
    report('After enqueuing an item', [ 7, 8, 1 ], []);

    $element2 = $q->enqueue_items({ items => [ 2 ] })->[0];
    $unprocessed++;
    report('After enqueuing a second item', [ 7, 8, 1, 2 ], []);

    $element0 = $q->enqueue_items({ items => [ 0 ] })->[0];
    $unprocessed++;
    report('After enqueuing a third item', [ 7, 8, 1, 2, 0 ], []);

    push @elements, $q->claim_items_nonblocking({ number_of_items => 1234 });
    $in_progress += $unprocessed;
    $unprocessed = 0;
    report('After evacuating the unprocessed queue', [], [ 7, 8, 1, 2, 0 ]);

    $q->mark_items_as_processed({ items => [ $element2, $element1, $element0 ] });
    $processed += 3;
    $in_progress -= 3;
    report('After processing some items in the working queue (out of order to test that as well)', [], [ 7, 8 ]);

    $q->mark_items_as_processed({ items => [ $items_in->[6], $items_in->[7] ] });
    $processed += 2;
    $in_progress -= 2;
    report('After processing the remaining items in the working queue', [], []);

    # Miscellaneous...
    dies_ok {
        $q->handle_expired_items([]);
    } 'handle_expired_items() correctly dies if called with something other than a hash reference.';
    dies_ok {
        $q->handle_expired_items({ timeout => 'blah' });
    } 'handle_expired_items() correctly dies if the "timeout" parameter is not a positive integer.';
    dies_ok {
        $q->handle_expired_items({ action => 'blah' });
    } 'handle_expired_items() correctly dies if the "action" parameter is not valid.';

    dies_ok {
        $q->handle_failed_items([]);
    } 'handle_failed_items() correctly dies if called with something other than a hash reference.';
    dies_ok {
        $q->handle_failed_items({ action => 'blah' });
    } 'handle_failed_items() correctly dies if the "action" parameter is not valid.';

    dies_ok {
        $q->process_failed_items([]);
    } 'process_failed_items() correctly dies if called with something other than a hash reference.';
    dies_ok {
        $q->process_failed_items({ max_count => 'blah' });
    } 'process_failed_items() correctly dies if the "max_count" parameter is not a positive integer.';
    dies_ok {
        $q->process_failed_items({ callback => 'blah' });
    } 'process_failed_items() correctly dies if the "callback" parameter is not a code reference.';

    dies_ok {
        $q->get_items_age([]);
    } 'get_items_age() correctly dies if called with something other than a hash reference.';
    dies_ok {
        $q->get_items_age({ subqueue_name => 'blah' });
    } 'get_items_age() correctly dies if the "subqueue_name" parameter is not valid.';
    dies_ok {
        $q->get_items_age({ direction => 'blah' });
    } 'get_items_age() correctly dies if the "direction" parameter is not valid.';
    dies_ok {
        $q->get_items_age({ items => 'blah' });
    } 'get_items_age() correctly dies if the "items" parameter is not an array reference.';
    dies_ok {
        $q->get_items_age({ items => [ 'blah' ] });
    } 'get_items_age() correctly dies if the "items" parameter contains anything other than queue items.';

    ok(
        !defined $q->peek_item({ subqueue_name => 'working' }),
        'peek_item() returns undef whenever the requested queue is empty.'
    );

    $q->process_failed_items({ max_count => 1234, callback => sub { push @failed, shift->data } });
    $failed -= 3;
    report('After process_failed_items()', [], [], []);
    is_deeply(\@failed, [ 1, 2, 0, 4, 5, 6 ], 'process_failed_items() works in the correct order and behaves correctly if passed too many items to proceess.');

    # Remember the original value, to restore everything at the end...
    my (undef, $max_memory) = $q->redis_handle->config_get('maxmemory');

    $q->redis_handle->config_set('maxmemory', 0);
    ok(
        !defined $q->percent_memory_used,
        'percent_memory_used() returns undef because "maxmemory" configuration option is not set.'
    );

    $q->redis_handle->config_set('maxmemory', 1);
    ok(
        $q->percent_memory_used =~ m/^\d+$/,
        'percent_memory_used() returns a number because "maxmemory" configuration option is set.'
    );

    # Restore the configuration value.
    $q->redis_handle->config_set('maxmemory', $max_memory);
}

1;
