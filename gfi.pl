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

use strict;
use warnings;

use feature ':5.10';

use JSON::XS;
use Digest::SHA1 qw(sha1);
use Fcntl;
use Devel::Size qw(total_size);

#use Compress::Zlib qw(compress);
use Deep::Hash::Utils qw(nest deepvalue);

use File::Path qw(make_path);
#use PerlIO::gzip;
use IO::Compress::Deflate qw(deflate $DeflateError);
require bytes;

binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

STDOUT->autoflush(1);

my $tree = {};
my $last_commit;
my $GIT = '.git/objects';


# create object directories
make_path( map { sprintf qq($GIT/%02x), $_ } (0 .. 255) );


while (my $line = <>) {
    if (substr($line, 0, 9) eq 'progress ') {
        print $line;
        next;
    }

    my $rev = decode_json($line);

    my @parts = split qr{/}, $rev->[2];
    
    nest($tree, @parts, {_sha1 => pack('H*', $rev->[1]), _mode => '100644'});

    while(@parts) {
        pop @parts;
        my $twig = deepvalue($tree, @parts);
        my $t = get_tree($twig);
        @$twig{qw(_mode _sha1)} = ('40000', $t->[0]);
        write_object($t);
    }

    my $commit = get_commit( $last_commit, $tree->{_sha1}, $rev->[0] );
    write_object( $commit );
    $last_commit = unpack('H*', $commit->[0]);
}

print STDERR "last commit: $last_commit\n";
print STDERR "size: ", total_size($tree), "\n";

sub write_object {
    my ($o) = @_;
    my $sha1 = unpack('H*', $o->[0]);

    my $path = join( q{/}, $GIT, substr($sha1, 0, 2), substr($sha1, 2) );

#    my $status = deflate \($o->[1]) => $path
#        or die "deflate failed: $DeflateError\n";
#=begin

    sysopen(my $out, $path, O_WRONLY | O_TRUNC | O_CREAT);
    syswrite $out, defl(\($o->[1]))
        or die "cannot write to '$path'";
    close($out)
        or die "cannot close '$path'";

#=cut

}

sub get_commit {
    my ($parent, $sha1, $msg) = @_;

    my $content = sprintf qq{tree %s\n%s%s},
        unpack('H*', $sha1),
        (defined $parent ? qq{parent $parent\n} : ''),
        $msg;
    my $c = sprintf qq{commit %d\x00%s}, bytes::length($content), $content;

    return [sha1($c), $c];
}

sub get_tree {
    my ($twig) = @_;

    use bytes;
    my @k = grep { $_ ne '_sha1' && $_ ne '_mode' } keys %$twig;
    my $tmpl =  qq{%s %s\x00%s} x @k;
    my @s = sort @k;
    my @v = map { $twig->{$_}->{_mode}, $_, $twig->{$_}->{_sha1}; } @s;
    my $content = sprintf $tmpl, @v;
        
    my $c = sprintf qq{tree %d\x00%s}, bytes::length($content), $content;
    return [sha1($c), $c];
}

sub defl {
    my ($t) = @_;
    my ($out1, $out2);

    use Compress::Raw::Zlib;
    my $err;
    state $x = Compress::Raw::Zlib::_deflateInit(
        0,
        Z_DEFAULT_COMPRESSION(),
        Z_DEFLATED(),
        MAX_WBITS(),
        MAX_MEM_LEVEL(),
        Z_DEFAULT_STRATEGY(),
        4096, ""
    );

    $err = $x->deflate($t, $out1);
    $err == Z_OK or die "cannot deflate object";
    
    $err = $x->flush($out2);
    $err == Z_OK or die "cannot finish object";

    $err = $x->deflateReset();
    $err == Z_OK or die "cannot reset object";

    return $out1 . $out2;
}


