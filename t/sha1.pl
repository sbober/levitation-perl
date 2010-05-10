#!/usr/bin/perl

use Faster;
use Benchmark qw(:all);
use Digest::SHA1;
use File::Slurp;

my $f = read_file('/usr/share/dict/ngerman');

my $r = timethese(-10, {
linus => sub {my $s = Faster::sha1($f);},
gisle => sub{my $s = Digest::SHA1::sha1($f);}
});

cmpthese($r);
