#!/usr/bin/perl
# This is my perl conversion of scytale's levitation project
# (http://github.com/scy/levitation).

use feature ':5.10';

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin";

use Carp;

# don't overwrite CORE::length; thus only use -> no
use bytes; no bytes;

use Regexp::Common qw(net);
use JSON::XS;
use Socket qw(inet_ntoa);
use Time::Piece;
use List::Util qw(min);

use CDB_File;


binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');


my %OPTS = (
    revs        => 0,
    committer   => 'Levitation-perl <lev@servercare.de>',
    depth       => 3,
    dir         => '.',
    current     => undef,
    help        => undef,
    max_gfi     => 1000000,
    gfi_cmd     => 'git fast-import',
);

handle_options();

$OPTS{tz} = opt('current') ? strftime('%z', localtime()) : '+0000';

open my $meta, '<', (opt('dir') . '/meta.json')
    or die "cannot open meta.json for reading";
    my $m_in = decode_json(do { local $/; <$meta> });
    $OPTS{revs} ||= $m_in->{maxrev};
    $OPTS{domain} = $m_in->{domain};
close($meta);

step2();


sub step2 {
    my $domain = opt('domain');
    my $gfi = get_gfi();

    printf {$gfi} "progress Step 2: creating %d commits\n", opt('revs');
    my $commit_id = 1;
    while (my $rev = next_rev()) {
        commit_times($rev);

        if ($commit_id % 100000 == 0) {
            print {$gfi} "progress revision $commit_id ($rev->{wtime})\n";
        }

        $rev->{msg} = sprintf "%s%s\n\n Levitation-perl of %d rev %d\n",
                    ($rev->{comment} // ''),
                    ((defined $rev->{minor} && $rev->{minor}) ? ' (minor)': ''),
                    $rev->{page_id}, $rev->{id};

        user($rev, $domain);
        path($rev);

        commit_to_gfi($gfi, $rev);
        $commit_id++;
    }

    print {$gfi} "progress all done!\n";
}

sub next_rev {
    state $revid = -1;
    state $DB;
    state $DIR = opt('dir');
    state $MAX = opt('revs');
    while (defined $revid && (!$MAX || $revid <= $MAX)) {
        $revid++;

        if ($revid % 4000000 == 0) {
            my $dbnr = int($revid / 4000000);
            #$DB->DESTROY() if defined $DB;
            #undef $DB;
            $DB = CDB_File->TIEHASH("$DIR/revs$dbnr.db") or last;
        }
        my $c = $DB->FETCH($revid);
        next if not defined $c;

        my %data = (id => $revid);
        @data{qw(uid isip username page_id namespace title timestamp comment
                 sha1 minor)}
        = @{ decode_json($c) };
        return \%data;
    }
    #$DB->DESTROY() if defined $DB;
    return;
}

sub commit_times {
    my ($rev) = @_;

    state $CURRENT = opt('current');
    $rev->{wtime}
    = Time::Piece->strptime( $rev->{timestamp}, '%Y-%m-%dT%H:%M:%SZ' )
                 ->strftime('%s');
    $rev->{ctime} = $CURRENT ? time() : $rev->{wtime};
}


sub user {
    my ($rev, $domain) = @_;

    my $ip;
    if ($rev->{isip}) {
        my $uid = $rev->{uid};
        $rev->{uid} = undef;
        $ip = inet_ntoa($uid);
    }

    my $uid = $rev->{uid};

    my $uname = $rev->{username} || $uid || $ip || "Unknown";

    my $email = defined $uid    ? sprintf("uid-%s@%s", $uid, $domain)
              : defined $ip     ? sprintf("ip-%s@%s", $ip, $domain)
              :                   sprintf("unknown@%s", $domain);

    $email = sprintf ("%s <%s>", $uname // $ip, $email);
    $rev->{user} = $email;
}

sub path {
    my ($rev) = @_;
    
    state $DEPTH = opt('depth');

    my @parts = ($rev->{namespace});
    # we want sane subdirectories
    push @parts, map { 
        $_ =~ s{([^0-9A-Za-z_])}{sprintf(".%x", ord($1))}e;
        $_;
    } split(//, substr($rev->{title}, 0, $DEPTH));

    $rev->{title} =~ s{/}{\x1c}g;
    push @parts, $rev->{title};

    $rev->{path} = join('/', @parts);
}

sub commit_to_gfi {
    my ($gfi, $rev) = @_;

    state $COMMITTER = opt('committer');
    state $TZ        = opt('tz');

    my $tmpl = qq{author %s %s +0000\ncommitter %s %s %s\n\n%s\n};

    my $commit = sprintf(
        $tmpl,
        $rev->{user}, $rev->{wtime},
        $COMMITTER, $rev->{ctime}, $TZ,
        $rev->{msg}
    );

    print {$gfi} encode_json([
        $commit,
        unpack('H*', $rev->{sha1}),
        $rev->{path}]), "\n";
}

=begin

sub commit_to_gfi {
    my ($gfi, $rev) = @_;

    state $COMMITTER = opt('committer');
    state $TZ        = opt('tz');

    my $tmpl = 
qq{commit refs/heads/master
author %s %s +0000
committer %s %s %s
data %d
%s
M 100644 %s %s
};

    print {$gfi} sprintf(
        $tmpl,
        $rev->{user}, $rev->{wtime},
        $COMMITTER, $rev->{ctime}, $TZ,
        bytes::length($rev->{msg}), $rev->{msg},
        unpack('H*', $rev->{sha1}),
        $rev->{path}
    );
}


=cut


sub get_gfi {
    my $cmd = opt('gfi_cmd');
    return \*STDOUT;
    #open(my $gfi, '|-:utf8', $cmd)
    #    or croak "error opening pipe to 'git fast-import': $!";

    #return $gfi;
}


sub handle_options {
    use Getopt::Long;

    my %NOPTS;
    my $result = GetOptions(
        'max|m=i'       => \$NOPTS{revs},
        'depth|d=i'     => \$NOPTS{depth},
        'tmpdir|t=s'    => \$NOPTS{dir},
        'current|c'     => \$NOPTS{current},
        'help|?'        => \$NOPTS{help},
    );
    usage() if !$result || $NOPTS{help};

    while (my($k, $v) = each %NOPTS) {
        $OPTS{$k} = $v if defined $v;
    }
}

sub opt {
    my ($key) = @_;
    croak "unknown option '$key'" if not exists $OPTS{$key};
    return $OPTS{$key};
}

sub usage {
    use File::Basename;
    my $name = basename($0);
    say STDERR qq{
$name - import MediaWiki dumps

Usage: bzcat pages-meta-history.xml.bz2 | \\
       $name [-m max_pages] [-t temp_dir|in_mem] [-d depth] [-h]

Options:
    -max
    -m max_revs     The number of revisions to dump.
                    (default = 0 (all revisions))

    -tmpdir
    -t temp_dir     The directory where temporary files should be written.
                    If this is 'in_mem', try to hold temp files in memory.
                    (default = '.')

    -depth
    -d depth        The depth of the directory tree under each namespace.
                    For depth = 3 the page 'Actinium' is written to
                    'A/c/t/Actinium.mediawiki'.
                    (default = 3)

    -current
    -c              Use the current time as commit time. Default is to use
                    the time of the wiki revision. NOTICE: Using this option
                    will create repositories that are guaranteed not to be
                    equal to other imports of the same MediaWiki dump.

    -help
    -h              Display this help text.
};

    exit(1);
}

1;
