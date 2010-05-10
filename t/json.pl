#!/usr/bin/perl

use strict;
use warnings;

use JSON::XS;
use Data::Dump qw(dump);
use File::Temp qw(tempfile);
use bytes ();
use IPC::Cmd qw(run);

binmode(STDIN, ':utf8');
my ($fh,$name) = tempfile(
    SUFFIX => '.pack',
    DIR => '.',
    UNLINK => 0,
);
my $count = 0;
while (<>) {
    my $a;
    eval {
        $a = decode_json($_);
    };
    if($@) {
        print "line: $., '" . dump($_) . "'\n";
    }
    my $out = dump($a);
    print {$fh} "$out\n";
    $count += bytes::length($out);
    $count++;

    if($count >= 2* 1024**2) {
        $fh->close;
        my $res = `tail -n 1 $name`;
        print "out: $res\n";
        $count = 0;
        ($fh,$name) = tempfile(
            SUFFIX => '.pack',
            DIR => '.',
            UNLINK => 0,
        );
    }
}

$fh->close;

