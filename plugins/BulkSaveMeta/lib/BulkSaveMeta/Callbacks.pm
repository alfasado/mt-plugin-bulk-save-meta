package BulkSaveMeta::Callbacks;

use strict;
use warnings;

sub initializer {
    require BulkSaveMeta::Proxy;
    return 1;
}

1;