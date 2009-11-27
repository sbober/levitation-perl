#!/usr/bin/perl
# This is my perl conversion of scytale's levitation project
# (http://github.com/scy/levitation).

use feature ':5.10';

use strict;
use warnings;

use threads;
use threads::shared;
use Thread::Queue;

use Carp;

# don't overwrite CORE::length; thus only require
require bytes;

#use LibXML_WMD;
use Regexp::Common qw(URI net);
use POSIX qw(strftime);
use List::Util qw(min first);
use Getopt::Long;
use TokyoCabinet;
use Storable qw(thaw nfreeze);
use Time::Piece;

binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');


my $PAGES       = 10;
my $COMMITTER   = 'Levitation-pl <lev@servercare.eu>';
my $DEPTH       = 3;
my $DIR         = '.';
my $CURRENT;
my $HELP;
my @NS;
my $ALL;

my $result = GetOptions(
    'max|m=i'       => \$PAGES,
    'depth|d=i'     => \$DEPTH,
    'tmpdir|t=s'    => \$DIR,
    'ns|n=s'        => \@NS,
    'all|a'         => \$ALL,
    'current|c'     => \$CURRENT,
    'help|?'        => \$HELP,
);
usage() if !$result || $HELP;

@NS = ('Main') if !@NS;

# FIXME: would like to use Time::Piece's strftime(), but it returns the wrong timezone
my $TZ = $CURRENT ? strftime('%z', localtime()) : '+0000';

my $filename = "$DIR/levit.db";

my $stream = \*STDIN;

# put the parsing in a thread and provide a queue to give parses back through
my $queue = Thread::Queue->new();
my $thr = threads->create(\&thr_parse, $stream, $queue, $PAGES, \@NS, $ALL);

# use TokyoCabinet BTree database as persistent storage
my $CACHE = TokyoCabinet::BDB->new() or die "db corrupt: new";
# sort keys as decimals
$CACHE->setcmpfunc($CACHE->CMPDECIMAL);
# use a large bucket
$CACHE->tune(128, 256, 3000000, 4, 10, $CACHE->TLARGE|$CACHE->TDEFLATE);
$CACHE->open($filename, $CACHE->OWRITER|$CACHE->OCREAT|$CACHE->OTRUNC) or die "db corrupt: open";

my $domain = $queue->dequeue();
$domain = "git.$domain";

my $c_rev = 0;
my $max_id = 0;

while (defined(my $page = $queue->dequeue()) ) {
    # record current revision and page ids to be able to provide meaningful progress messages
    if ($page->{new}) {
        printf("progress processing page '%s:%s'  $c_rev / < $max_id\n", $page->{namespace}, $page->{title});
    }
    my $revid = $page->{revision_id};
    $max_id = $revid if $revid > $max_id;

    # extract all relevant data
    my %rev = (
        user    => user($page, $domain),
        comment => ($page->{comment} // "") . ( $page->{minor} ? " (minor)" : ""),
        timestamp    => $page->{timestamp},
        pid     => $page->{id},
        ns      => $page->{namespace},
        nsid    => $page->{nsid},
        title   => $page->{title},
    );

    # persist the serialized data with rev id as reference
    $CACHE->put("$revid", nfreeze(\%rev)) or die "db corrupt: put";

    # and give the text to stdout, so git fast-import has something to do
    my $text = $page->{text};
    print sprintf qq{blob\nmark :%s\ndata %d\n%s\n}, $revid, bytes::length($text), $text;
    $c_rev++;
}
# we don't need the worker thread anymore. The input can go, too.
$thr->join();
close($stream);

$CACHE->close() or croak "db can't be closed: $!";
$CACHE = TokyoCabinet::BDB->new() or croak "db new: $!";
$CACHE->open($filename, $CACHE->OREADER) or croak "db can't be reopened: $!";


# go over the persisted metadata with a cursor
my $cur = TokyoCabinet::BDBCUR->new($CACHE) or croak "can't get a cursor on db: $!";
$cur->first();
say "progress processing $c_rev revisions";

my $commit_id = 1;
while (defined(my $revid = $cur->key())){
    if ($commit_id % 100000 == 0) {
        say "progress revision $commit_id / $c_rev";
    }

    my $rev = thaw($cur->val());
    my $msg = "$rev->{comment}\n\nLevit.pl of page $rev->{pid} rev $revid\n";

    my $branch = $rev->{nsid} || 'master';

    my @parts = $rev->{ns};
    # we want sane subdirectories
    for my $i (0 .. min( length($rev->{title}), $DEPTH) -1  ) {
        my $c = substr($rev->{title}, $i, 1);
        $c =~ s{([^0-9A-Za-z_])}{sprintf(".%x", ord($1))}eg;
        push @parts, $c;
    }

    $rev->{title} =~ s{/}{\x1c}g;
    push @parts, $rev->{title};
    my $wtime = Time::Piece->strptime($rev->{timestamp}, '%Y-%m-%dT%H:%M:%SZ')->strftime('%s');
    my $ctime = $CURRENT ? time() : $wtime;

    print sprintf
q{commit refs/heads/%s
author %s %s +0000
committer %s %s %s
data %d
%s
M 100644 :%d %s
},
    $branch, $rev->{user}, $wtime, $COMMITTER, $ctime, $TZ, bytes::length($msg), $msg, $revid, join('/', @parts);

    $commit_id++;
    $cur->next();
}

$CACHE->close() or croak "can't close db: $!";
say "progress all done! let git fast-import finish ....";

# get an author string that makes git happy and contains all relevant data
sub user {
    my ($page, $domain) = @_;

    my $uid = $page->{userid};
    my $ip = $page->{ip};
    $ip = "255.255.255.255" if !defined $ip || $ip !~ $RE{net}{IPv4};
    my $uname = $page->{username} || $page->{userid} || $ip || "Unknown";

    my $email = defined $uid    ? sprintf("uid-%s@%s", $uid, $domain)
              : defined $ip     ? sprintf("ip-%s@%s", $ip, $domain)
              :                   sprintf("unknown@%s", $domain);

    $email = sprintf ("%s <%s>", $uname // $ip, $email);
    return $email;
}

# parse the $stream and put the result to $queue
sub thr_parse {
    my ($stream, $queue, $MPAGES, $MNS, $MALL) = @_;
    my $revs = LibXML_WMD->new(FD => $stream);

    # give the site's domain to the boss thread
    my (undef, undef, $domain) = ($revs->{base} =~ $RE{URI}{HTTP}{-keep});
    $queue->enqueue($domain);
    
    my $c_page = 0;
    my $current = "";
    while (my $rev = $revs->next) {
        next if !$MALL && !first { $rev->{namespace} eq $_ } @$MNS;
        # more than max pages?
        if ($current ne $rev->{id}) {
            $current = $rev->{id};
            $c_page++;
            $rev->{new} = 1;
            last if $MPAGES > 0 && $c_page > $MPAGES;
        }
        # make threads::shared happy (initializes shared hashrefs);
        my $h = &share({});
        %$h = %$rev;
        while ($queue->pending() > 10000) {
            threads->yield();
        }

        $queue->enqueue($h);
    }

    # give an undef to boss thread, to signal "we are done"
    $queue->enqueue(undef);
    return;
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
                    multiple times. Default is to just import "Main".

    -all
    -a              Import all namespaces. Default is "no".

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



package LibXML_WMD;

use strict;
use warnings;
use XML::LibXML::Reader;
use XML::LibXML::XPathContext;

sub new {
    my ($class) = shift @_;

    ##### Configuration section #####
    my $NS = { x => 'http://www.mediawiki.org/xml/export-0.4/' };

    my %defaults = ();
    my %DEFS = (
        '//x:revision/x:id'                     => 'revision_id',
        '//x:revision/x:comment'                => 'comment',
        '//x:revision/x:minor'                  => 'minor',
        '//x:revision/x:text'                   => 'text',
        '//x:revision/x:timestamp'              => 'timestamp',
        '//x:revision/x:contributor/x:id'       => 'userid',
        '//x:revision/x:contributor/x:username' => 'username',
        '//x:revision/x:contributor/x:ip'       => 'ip',
    );
    my %PPATS = (
        title   => XML::LibXML::Pattern->new('//x:page/x:title',    $NS),
        id      => XML::LibXML::Pattern->new('//x:page/x:id',       $NS),
        rev     => XML::LibXML::Pattern->new('//x:revision',        $NS),
    );
    my %PATS = map { $_ => XML::LibXML::Pattern->new($_, $NS) } keys %DEFS;

    my $pattern = XML::LibXML::Pattern->new(join(q{|}, '//x:revision','//x:page/x:id','//x:page/x:title', keys %DEFS), $NS);
    my $si_pattern = XML::LibXML::Pattern->new('//x:siteinfo', $NS);
    ##### Configuration end #####


    my $reader = XML::LibXML::Reader->new( @_ );
    my $st = $reader->nextPatternMatch($si_pattern);
    die "cannot find siteinfo section" if $st <= 0;

    my $si = $reader->copyCurrentNode(1);

    my $XPC = XML::LibXML::XPathContext->new;
    $XPC->registerNs(%$NS);
    $XPC->setContextNode($si);

    my %self = (
        PATS        => \%PATS,
        PPATS       => \%PPATS,
        DEFS        => \%DEFS,
        pattern     => $pattern,
        page        => {},
        list        => [],
        reader      => $reader,
        base        => $XPC->findvalue("x:base"),
        sitename    => $XPC->findvalue("x:sitename"),
        _namespaces => {map { $_->textContent => $_->findvalue('@key') } $XPC->findnodes("x:namespaces/x:namespace") },
    );
    $self{nsre} = join( q{|}, map { quotemeta($_) } keys %{$self{_namespaces}} );

    return bless \%self, $class;
}

sub next {
    my ($self) = @_;
    my $reader = $self->{reader};

    my %data = ();
    my %page = %{ $self->{page} };
    
    ELT:
    while ($reader->nextPatternMatch($self->{pattern}) > 0 ) {

        next ELT unless $reader->nodeType() == 1;

        if ( $reader->matchesPattern($self->{PPATS}{title}) ) {
            
            $reader->read;
            my $value = $reader->value;
            my ($ns, $title);

            if ($value =~ m/^($self->{nsre}):(.+)/) {
                ($ns, $title) = ($1, $2);
            }
            else {
                ($ns, $title) = ('Main', $value);
            }
            
            my %h = (title => $title, namespace => $ns, nsid => $self->{_namespaces}{$ns} || 0);
            if (!%page) {
                %page = %h;
            }
            $self->{page} = \%h;

        }
        elsif ( $reader->matchesPattern($self->{PPATS}{id}) ) {

            $reader->read;
            my $value = $reader->value;
            $page{id} ||= $value;
            $self->{page}{id} = $value;

        }
        elsif ( $reader->matchesPattern($self->{PPATS}{rev}) ) {

            if (%data) {
                last ELT;
                # print Dumper({%page, %data});
            }

        }
        else {
            IN_REV:
            while (my ($k, $v) = each %{ $self->{PATS} }) {
                if ($reader->matchesPattern($v)) {
                    $reader->read;
                    $data{ $self->{DEFS}{$k} } = $reader->value;
                    #print $reader->name, ": ", $reader->readInnerXml, "\n";
                    # reset 'each' iterator
                    keys %{ $self->{PATS} };
                    last IN_REV;
                }
            }
        }
    
    }
    if (%data) {
        push @{ $self->{list} }, {%page, %data};
    }

    if (@{ $self->{list} }) {
        return( shift @{ $self->{list} } );
    }

    $reader->close();
    return;
}
1;
