#!/usr/bin/perl
# This is my perl conversion of scytale's levitation project
# (http://github.com/scy/levitation).

use feature ':5.10';

use strict;
use warnings;

use threads;
use threads::shared;
use Thread::Queue;

use FindBin;
use lib "$FindBin::Bin";
use Faster;

use Carp;

# don't overwrite CORE::length; thus only use -> no
use bytes; no bytes;

use Regexp::Common qw(URI);
use List::Util qw(min first);
use JSON::XS;
use Socket qw(inet_aton);

use PrimitiveXML;
use CDB_File;


binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');


my %OPTS = (
    pages       => 10,
    committer   => 'Levitation-perl <lev@servercare.de>',
    depth       => 3,
    dir         => '.',
    current     => undef,
    help        => undef,
    max_gfi     => 1000000,
    gfi_cmd     => 'git fast-import --depth=4000',
    ns          => [],
);


handle_options();
work();

sub work {

    # create thread and queue for the persister
    my $queue       = Thread::Queue->new();
    my $persister   = threads->create(\&persist, $queue);

    my $stream      = \*STDIN;
    
    # create thread and queue for the parser
    my $pqueue      = Thread::Queue->new();
    my $parse       = threads->create(\&get_revs, $stream, $pqueue);


    my $domain      = $pqueue->dequeue;

    my $count_rev   = 0;
    my $count_page  = 0;
    my $max_id      = 0;

    my $gfi         = get_gfi();

    while (my $rev = $pqueue->dequeue) {
        $count_rev++;

        if ($rev->{new}) {
            $count_page++;

            printf {$gfi} "progress processing page '%s%s' %d / < %d\n",
                $rev->{namespace} ne 'Main' ? "$rev->{namespace}:" : "",
                $rev->{title}, $count_rev, $max_id;

        }

        my $revid   = $rev->{revision_id};
        $max_id     = $revid if $revid > $max_id;

        # feed the text to git fast-import
        blob_to_gfi( $gfi, $rev );

        sleep 1 while $queue->pending > 1000;
        $queue->enqueue($rev);

    }
    $queue->enqueue(undef);
    $persister->join;
    $parse->join;
    printf {$gfi} "progress step 1 done, written %d pages / %d revisions\n",
        $count_page, $count_rev;

    open my $meta, '>', (opt('dir') . "/meta.json")
        or die "cannot open meta.json for writing: $!";

    print {$meta} encode_json({
            maxrev => $max_id,
            domain => $domain,
    });
    close($meta);
}


sub blob_to_gfi {
    my ($gfi, $rev) = @_;

    $rev->{text}    //= "";
    $rev->{len}       = bytes::length($rev->{text});

    print {$gfi} sprintf(qq{blob\ndata %d\n%s\n}, $rev->{len}, $rev->{text});
}


sub get_revs {
    my ($stream, $queue) = @_;

    my $parser      = PrimitiveXML->new(handle => $stream);
    my $domain      =($parser->{base} =~ $RE{URI}{HTTP}{-keep})[2];

    $queue->enqueue($domain);

    my $count_page  = 0;
    my $MAX_PAGES   = opt('pages');

    my $current_page_id = '';
    my $NS = opt('ns');

    REV:
    while (my $rev = $parser->next) {
        next if @$NS && !first { $rev->{namespace} eq $_ } @$NS;

        if ($current_page_id ne $rev->{id}) {
            $current_page_id = $rev->{id};
            $rev->{new} = 1;
            $count_page++;
            last if $MAX_PAGES > 0 && $count_page > $MAX_PAGES;
        }
        my $h = &share({});
        %$h = %$rev;
        sleep 1 while $queue->pending > 1000;
        $queue->enqueue($h);

    }
    $queue->enqueue(undef);
    return;
}




sub persist {
    my ($queue) = @_;

    my $DIR = opt('dir');
    my %DB;


    # partition the DB based on revision id and so that one DB slice
    # doesn't hold more than 4M records. DB 0 gets revs 1 - 3999999, DB 1
    # gets revs 4000000 - 7999999, ...
    my $count = 0;
    while (my $data = $queue->dequeue) {

        my $dbnr = int($data->{revision_id} / 4000000);

        # create the DB slice if it doesn't exist
        if (!exists $DB{"revs$dbnr"}) {
            $DB{"revs$dbnr"} = CDB_File->new(
                "$DIR/revs$dbnr.db", "t$dbnr.$$"
            )
                or croak "cannot create DB revs$dbnr: $!";
        }

        # extract user information based on what's available
        my $ip      = $data->{ip} // '';
        $ip         = $ip =~ /^[\d.]+$/ ? inet_aton($ip) : undef;
        my $uid     = $data->{userid} // $ip // -1;
        my $isip    = defined $ip && ($uid eq $ip);

        my $stxt = sprintf(qq{blob %d\x00%s}, $data->{len}, $data->{text});
        my $sha1 = Faster::sha1($stxt);

        # serialize the data to JSON and put it in the DB
        my $rev = encode_json([
            $uid, $isip, 
            @{$data}{qw/username id namespace title timestamp comment/},
            $sha1,
            defined($data->{minor})
        ]);
        $DB{"revs$dbnr"}->insert($data->{revision_id}, $rev);

        $count++;
    }

    # finish and close all DBs
    for my $k (keys %DB) {
        my $db = $DB{$k};
        $db->finish();
    }

    return;
}


sub get_gfi {
    my $cmd = opt('gfi_cmd');
    open(my $gfi, '|-:utf8', $cmd)
        or croak "error opening pipe to 'git fast-import': $!";

    return $gfi;
}

sub handle_options {
    use Getopt::Long;

    my %NOPTS;
    my $result = GetOptions(
        'max|m=i'       => \$NOPTS{pages},
        'depth|d=i'     => \$NOPTS{depth},
        'tmpdir|t=s'    => \$NOPTS{dir},
        'db=s'          => \$NOPTS{db},
        'ns|n=s@'       => \$NOPTS{ns},
        'current|c'     => \$NOPTS{current},
        'help|?'        => \$NOPTS{help},
        'one|o'         => \$NOPTS{one},
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
    -m max_pages    The number of pages (with all their revisions) to dump.
                    (default = 10)

    -tmpdir
    -t temp_dir     The directory where temporary files should be written.
                    If this is 'in_mem', try to hold temp files in memory.
                    (default = '.')

    -depth
    -d depth        The depth of the directory tree under each namespace.
                    For depth = 3 the page 'Actinium' is written to
                    'A/c/t/Actinium.mediawiki'.
                    (default = 3)

    -ns
    -n namespace    The namespace(s) to import. The option can be given
                    multiple times. Default is to import all namespaces.

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





