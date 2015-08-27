package Queue::Q::ReliableFIFO::ItemNG2000TopFun;

use Data::Dumper;
use strict;
use warnings;

sub data          { return $_[0]->{payload} }
sub time_created  { return $_[0]->{metadata}{time_created}  }
sub time_enqueued { return $_[0]->{metadata}{time_enqueued} }
sub last_error    { return $_[0]->{metadata}{last_error}    }
sub process_count { return $_[0]->{metadata}{process_count} } # amount of times a single consumer attempted to handle the item
sub bail_count    { return $_[0]->{metadata}{bail_count}    } # amount of times process_count exceeded its threshold

sub new {
    my ($class, $item) = @_;

    bless $item, $class;

    return $item;
}

1;
