package Git::Common;

use feature ':5.10';

use strict;
use warnings;

use Carp;

sub repo {
    my ($path) = @_;
    $path //= '';
    my $base = $ENV{GIT_DIR} // '.git';
    croak "'$base' is not a readable git repository" if !(-r -d "$base/objects");
    
    return "$base/$path"

}

1;
