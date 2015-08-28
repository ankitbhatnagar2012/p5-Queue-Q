#!/usr/bin/env perl

use 5.14.2;
use strict;
use warnings;

use lib '../lib';

use Data::UUID::MT;
use Queue::Q::ReliableFIFO::Redis;
use Queue::Q::ReliableFIFO::RedisNG2000TopFun;
use Time::HiRes;
use POSIX qw/ WNOHANG strftime /;

our $log_fh;
our $UUID = Data::UUID::MT->new(version => 4);

our $TEST_CLASS;
BEGIN {
    $TEST_CLASS = $ENV{TEST_CLASS} ? sprintf('Queue::Q::ReliableFIFO::%s', $ENV{TEST_CLASS}) : 'Queue::Q::ReliableFIFO::RedisNG2000TopFun';
}

use constant { 
    TEST_CLASS => $TEST_CLASS,
};

sub say_with_time {
    my ($seconds, $microseconds) = split /[.]/ => Time::HiRes::time();
    $microseconds = sprintf '%05d', $microseconds;
    $log_fh->say( strftime("[%F %T.$microseconds] [$$] ", localtime) . $_[0] );
}

sub main {
    my ($mode) = @_;

    $mode =~ s/[-][-]//;

    # open $log_fh, '>>', "$mode.log";
    open $log_fh, '>>', \*STDOUT;
    $log_fh->autoflush(1);

    my %workers = (
        producer => \&run_in_producer,
        consumer => \&run_in_consumer,
        cleaner  => \&run_in_cleaner,
    );

    my %max_workers = (
        producer => $ENV{MAX_WORKERS} || 30,
        consumer => $ENV{MAX_WORKERS} || 60,
        cleaner  => 1,
    );

    say_with_time "$TEST_CLASS $mode parent starting up";

    my @children;
    my @exited;

    while (1) {
        
        while ( @children < $max_workers{$mode} ) {
            my $pid = fork();
            if ( $pid ) {
                push @children, $pid;
            }
            else {
                # open $log_fh, '>>', "$mode.log";
                # $log_fh->autoflush(1);
                $workers{$mode}->();
                exit();
            }
        }

        if ( @children ) {
            foreach (1 .. $max_workers{$mode}) {
                my $kid = waitpid(-1, WNOHANG);
                if ($kid) {
                    $kid and push @exited, $kid;
                    say_with_time "$TEST_CLASS $mode kid $kid exited";
                    @children = grep { $_ != $kid } @children;
                }
            }
        }

        say_with_time sprintf '%s %s parent is currently tracking %d children', $TEST_CLASS, $mode, int(@children);

        if ( -f "/tmp/stop" and $max_workers{$mode} ) {
            say_with_time "$TEST_CLASS $mode parent disabling spawning";
            $max_workers{$mode} = 0;
        }
        elsif ( -f _ and not @children) {
            say_with_time "$TEST_CLASS $mode parent exiting";
            last;
        }

        Time::HiRes::sleep 0.5;
    }

}

sub run_in_producer {
    say_with_time "$TEST_CLASS producer child starting up";

    my $q = $TEST_CLASS->new(server => 'localhost', port => 6379, queue_name => 'throughput_test');
    my $r = $q->can('redis_handle') ? $q->redis_handle : $q->redis_conn;

    my $id = 1;
    until ( -f '/tmp/stop' ) {
        if ( ( $r->info('memory')->{used_memory} / 1024 / 1024 ) > 100 ) {
            say_with_time "$TEST_CLASS producer backing off";
            Time::HiRes::sleep(0.1);
            next;
        }
        my @items = map { sprintf q|{ pid: %d, counter: %d, UUID: %s }|, $$, $id++, $UUID->create_hex } ( 1 .. 1000 );
        say_with_time "$TEST_CLASS enqueueing 1000 items";
        # eval { do { $q->enqueue_item($_); print "\n" } for @items } or do { warn "enqueue error: $@" };
        eval { $q->enqueue_item(@items); 1; } or do { warn "enqueue error: $@" };
        say_with_time "$TEST_CLASS producer child inserted batch of 1000";
        last if rand(100) > 95;
        last if time() - $^T > rand(20) + 20;
        sleep 1;
    }

    $id--;

    say_with_time sprintf '%s producer child exiting, did %d items at a rate of %.02f/sec', TEST_CLASS, $id, $id / ( Time::HiRes::time() - $^T );
}

sub run_in_consumer {
    say_with_time "$TEST_CLASS consumer child starting up";
    my $q = $TEST_CLASS->new(server => 'localhost', port => 6379, queue_name => 'throughput_test');

    my $done = 0;

    until ( -f '/tmp/stop' ) {
        my $thing;

        my $item = $q->claim_item();

        if ( TEST_CLASS eq 'Queue::Q::ReliableFIFO::RedisNG2000TopFun' and $item ) {
            $done++;
            # print "\n";
            if (rand(100) < 20) {
                $q->requeue_busy_error('Chaos monkey', $item);
            } else {
                $q->mark_item_as_processed($item);
            }
        }
        elsif ( TEST_CLASS eq 'Queue::Q::ReliableFIFO::Redis' and $item ) {
            $done++;
            # print "\n";
            $q->mark_item_as_done($item);
        }

        if ( $item and $done % 5 == 0 ) {
            say_with_time sprintf '%s consumer child handled %d items at a rate of %.02f/sec', $TEST_CLASS, $done, $done / ( Time::HiRes::time() - $^T );
        }

        last if rand(10000) > 9990;
    }

    say_with_time sprintf '%s consumer child exiting, did %d items at a rate of %.02f/sec', $TEST_CLASS, $done, $done / ( Time::HiRes::time() - $^T );
}

sub run_in_cleaner {
    say_with_time "$TEST_CLASS cleaner starting up";
    my $q = $TEST_CLASS->new(server => 'localhost', port => 6379, queue_name => 'throughput_test');

    my $done = 0;

    until ( -f '/tmp/stop' ) {
        my $thing;

        my ($count, $items) = $q->remove_failed_items();

        if ( TEST_CLASS eq 'Queue::Q::ReliableFIFO::RedisNG2000TopFun' and $count ) {
            $done += $count;
            say_with_time ("Removed $count items");
        }

        last if rand(10000) > 9990;
    }

    say_with_time sprintf '%s cleaner child exiting, did %d items at a rate of %.02f/sec', $TEST_CLASS, $done, $done / ( Time::HiRes::time() - $^T );
}

main(@ARGV);
