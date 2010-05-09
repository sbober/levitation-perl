#!/usr/bin/perl

# take a stream of JSON strings or progress messages on STDIN
# and writes objects directly to the repo
#
# JSON format [ commit string , sha1 , file path ]
#
# commit string: everything but the "tree" and "parent" lines
# and the object header of a git commit object
#
# sha1: the 20 bytes binary sha1 of the modified | added file
#
# file path: the '/' seperated path of the file starting at the
# top tree

use feature ':5.10';

use strict;
use warnings;
require bytes;

use FindBin;
use lib "$FindBin::Bin";

use JSON::XS;
use Digest::SHA1 qw(sha1);
use Deep::Hash::Utils qw(nest deepvalue);

use Encode;

use Git::Tree;
use Git::Pack;
use Git::Common;


binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');
binmode(STDIN, ':utf8');

STDOUT->autoflush(1);

my %OPTS = (
    pack_size => int(600 * 1024**2),
    delta_depth => 50,
);

my $tree = {};
my $last_commit;
my $GIT = '.git/objects';
my $pack = Git::Pack->new;
my %may_delta;

while (my $line = <>) {
    if (substr($line, 0, 9) eq 'progress ') {
        print $line;
        next;
    }

    my $rev = decode_json($line);

    my @path = split qr{/}, Encode::encode_utf8($rev->[2]);
    my $file = pop @path;

    my $twig = get_tree($tree, @path);
    $twig->{_tree}->add(['100644', $file, pack('H*',$rev->[1]) ]  );
    
    my $sha1;
    while(@path) {
        $sha1 = write_tree($twig, \@path);
        my $dir = pop @path;
        $twig = get_tree($tree, @path);
        $twig->{_tree}->add(['40000', $dir, $sha1]);
    }
    $sha1 = write_tree($twig, \@path);

    my $commit = get_commit( $last_commit, $sha1, Encode::encode_utf8($rev->[0]) );
    my ($bin, $ofs) = $pack->maybe_write('commit', $commit);


    $last_commit = unpack('H*', $bin);

    if ($pack->{outbytes} >= $OPTS{pack_size}) {
        $pack->breakpoint;
        undef %may_delta;
    };
}

$pack->close;

open my $ref, '>', Git::Common::repo('refs/heads/master') or die 'cannot open "master"';
print {$ref} $last_commit or die 'cannot write to "master"';
close($ref) or die 'cannot close "master"';



sub get_tree {
    my ($tree, @path) = @_;
    my $t = deepvalue($tree, @path);
    if (!$t) {
        $t = {
            _tree => Git::Tree->new,
            _sha1 => undef,
            _ofs => undef
        };
        nest($tree, @path, $t);
    }
    if (!$t->{_tree}) {
        $t->{_tree} = Git::Tree->new;
        $t->{_sha1} = undef;
        $t->{_ofs} = undef;
    }
    return $t;
}

sub write_tree {
    my ($twig, $path_ref) = @_;

    my $path = join( '/', @$path_ref );
    if ($may_delta{$path} && $may_delta{$path} < $OPTS{delta_depth} && $twig->{_sha1} && $twig->{_ofs}) {
        my $diff = $twig->{_tree}->get_diff;
        my $obj = $twig->{_tree}->get_object;
        my $delta = Git::Pack::create_delta($twig->{_old}, \$obj, $diff);

        my ($sha1, $ofs) = $pack->delta_write('tree', $obj, $delta, $twig->{_ofs});
        $twig->{_sha1} = $sha1;
        $twig->{_ofs} = $ofs;
        $twig->{_old} = bytes::length($obj);
        $may_delta{$path}++;
    }
    else {
        my $obj = $twig->{_tree}->get_object;
        my ($sha1, $ofs) = $pack->maybe_write('tree', $obj);
        $twig->{_sha1} = $sha1;
        $twig->{_ofs} = $ofs;
        $twig->{_old} = bytes::length($obj);
        $may_delta{$path} = 1;
    }
    my $sha1 = $twig->{_sha1};
    $twig->{_tree}->reset;
    return $sha1;
}



sub get_commit {
    my ($parent, $sum, $msg) = @_;

    my $content = sprintf qq{tree %s\n%s%s},
        unpack('H*', $sum),
        (defined $parent ? qq{parent $parent\n} : ''),
        $msg;

    return $content;
}



