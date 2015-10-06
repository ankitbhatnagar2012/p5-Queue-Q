use strict;
use warnings;

use File::Spec;
use Test::More;
use Test::Fatal qw/dies_ok/;

use lib (-d 't' ? File::Spec->catdir(qw(t lib)) : 'lib');
use Queue::Q::Test;
use Queue::Q::TestReliableFIFO::RedisNG2000TopFun;

use Queue::Q::ReliableFIFO::RedisNG2000TopFun;

my ($host, $port) = get_redis_connect_info();
skip_no_redis() if not defined $host;

dies_ok {
    Queue::Q::ReliableFIFO::RedisNG2000TopFun->new(
        server => $host,
        port => $port
    );
} 'The constructor correctly dies when you fail to pass all the required options.';

dies_ok {
    Queue::Q::ReliableFIFO::RedisNG2000TopFun->new(
        [ ]
    );
} 'The constructor correctly dies when you pass anything but a hash reference as the second parameter.';

dies_ok {
    Queue::Q::ReliableFIFO::RedisNG2000TopFun->new({
        server => $host,
        port => $port,
        blah => 'blah' # What?!
    });
} 'The constructor correctly dies when you pass an unknown option.';

my $queue_name = 'test_redis2000';
my $q = Queue::Q::ReliableFIFO::RedisNG2000TopFun->new({
    server => $host,
    port => $port,
    queue_name => $queue_name,
    busy_expiry_time => 1 # For testing expiration of items while being handled.
});
isa_ok($q, "Queue::Q::ReliableFIFO");
isa_ok($q, "Queue::Q::ReliableFIFO::RedisNG2000TopFun");

my $q_another_instance = Queue::Q::ReliableFIFO::RedisNG2000TopFun->new({
    server => $host,
    port => $port,
    queue_name => $queue_name,
    busy_expiry_time => 1 # For testing expiration of items while being handled.
});

isnt($q, $q_another_instance, "Reinstantiating same queue objects should not be the same"); # comparing stringified instances only

is( $q->server, $host,           'Testing Class::XSAccessor->server getter.'       );
is( $q->port, $port,             'Testing Class::XSAccessor-port getter.'          );
is( $q->queue_name, $queue_name, 'Testing Class::XSAccessor->queue_name getter.'   );

my $requeue_limit = $q->requeue_limit;
my $busy_expiry_time = $q->busy_expiry_time;
my $claim_wait_timeout = $q->claim_wait_timeout;

$q->set_requeue_limit($requeue_limit);
$q->set_busy_expiry_time($busy_expiry_time);
$q->set_claim_wait_timeout($claim_wait_timeout);

is( $q->requeue_limit,      $requeue_limit,         'Testing Class::XSAccessor->requeue_limit setter.'     );
is( $q->busy_expiry_time,   $busy_expiry_time,      'Testing Class::XSAccessor->busy_expiry_time setter.'  );
is( $q->claim_wait_timeout, $claim_wait_timeout,    'Testing Class::XSAccessor->claim_wait_timeout settes.');

Queue::Q::TestReliableFIFO::RedisNG2000TopFun::test_reliable_fifo($q);

done_testing();
