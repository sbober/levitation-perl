#!/usr/bin/perl
# This is my perl conversion of scytale's levitation project
# (http://github.com/scy/levitation).
#
# mainly done because of DB_File

use feature ':5.10';

use strict;
use warnings;
use Carp;
require bytes;

use Parse::MediaWikiDump;
use Regexp::Common qw(URI net);
use POSIX qw(strftime);
use POSIX::strptime;
use List::Util qw(min);
use Getopt::Long;

binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');


my $PAGES       = 10;
my $COMMITTER   = 'Levitation-pl <lev@servercare.eu>';
my $DEPTH       = 3;
my $DIR         = '.';
my $HELP;

my $result = GetOptions(
    'max|m=i'       => \$PAGES,
    'depth|d=i'     => \$DEPTH,
    'tmpdir|t=s'    => \$DIR,
    'help|?'        => \$HELP,
);
usage() if !$result || $HELP;



my $TZ = strftime('%z', localtime());

my $CACHE = Tools::TieCache::get({
    dir     => '.',
    multi   => 1,
    func    =>
        sub {
            my($k1, $k2) = @_;
            $k1 <=> $k2;
        },
    type    => 'btree',
    unlink  => 0,
});


my $stream = \*STDIN;

my $pmwd = Parse::MediaWikiDump->new;
my $revs = $pmwd->revisions($stream);

my (undef, undef, $domain) = ($revs->base =~ $RE{URI}{HTTP}{-keep});
$domain = "git.$domain";

my $c_page = 0;
my $c_rev = 0;
my $current = "";
my $max_id = 0;
while (defined(my $page = $revs->next)) {
    if ($current ne $page->id) {
        $current = $page->id;
        $c_page++;
        last if $PAGES > 0 && $c_page > $PAGES;
        printf("progress processing page '%s'\n", $page->title); 
    }

    my $revid = $page->revision_id;
    $max_id = $revid if $revid > $max_id;

    my %rev = (
        user    => user($page, $domain),
        comment => ($page->{DATA}->{comment} // "") . ( $page->minor ? " (minor)" : ""),
        timestamp    => $page->timestamp,
        pid     => $page->id,
        ns      => $page->namespace || "Main",
        title   => ($page->title =~ /:/) ?  (split(/:/, $page->title, 2))[1] : $page->title,
    );

    $CACHE->{$revid} = \%rev;

    my $text = ${$page->text};
    print sprintf qq{blob\nmark :%s\ndata %d\n%s\n}, $revid, bytes::length($text), $text;
    $c_rev++;
}

close($stream);

my $commit_id = 1;

say "progress processing $c_rev revisions";

while (my ($revid, $rev) = each %$CACHE ) {
    if ($commit_id % 5000 == 0) {
        say "progress revision $commit_id / $c_rev";
    }

    my $msg = "$rev->{comment}\n\nLevit.pl of page $rev->{pid} rev $revid\n";
    my $from = $commit_id > 1 ? sprintf("from :%d\n", $commit_id - 1) : '';
    my @parts = ($rev->{ns});
    
    for my $i (0 .. min( length($rev->{title}), $DEPTH) -1  ) {
        my $c = substr($rev->{title}, $i, 1);
        $c =~ s{([^0-9A-Za-z_])}{sprintf(".%x", ord($1))}eg;
        push @parts, $c;
    }
    $rev->{title} =~ s{([^0-9A-Za-z :\.()_-])}{sprintf(".%x", ord($1))}eg;
    push @parts, $rev->{title};
    my $time = strftime('%s', POSIX::strptime($rev->{timestamp}, '%Y-%m-%dT%H:%M:%SZ'));

    print sprintf
q{commit refs/heads/master
mark :%d
author %s %s +0000
committer %s %s %s
data %d
%s
M 100644 :%d %s.mediawiki
},
    $commit_id, $rev->{user}, $time, $COMMITTER, time(), $TZ, bytes::length($msg), $msg, $revid, join('/', @parts);

    $commit_id++;
}

sub user {
    my ($page, $domain) = @_;

    my $uid = $page->userid;
    my $ip = $page->{DATA}->{ip};
    $ip = "255.255.255.255" if !defined $ip || $ip !~ $RE{net}{IPv4};
    my $uname = $page->username;

    my $email = defined $uid    ? sprintf("uid-%s@%s", $uid, $domain)
              : defined $ip     ? sprintf("ip-%s@%s", $ip, $domain)
              :                   "";
    $email = sprintf ("%s <%s>", $uname // $ip, $email);
    return $email;
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

    -help
    -h              Display this help text.
};

    exit(1);
}

1;


package Tools::TieCache;

use strict;
use warnings;

use Carp;
use DB_File;
use File::Temp qw( tempdir tempfile );



use Storable qw(freeze thaw);

$Storable::canonical = 1;


sub get {
    my %defaults = (
        dir         => 'in_mem',
        multi       => 0,
        unlink      => 1,
        type        => 'btree',
    );
    my ($conf_ref) = @_;
    my %config = (%defaults, %$conf_ref);
    my $filename;


    if ($config{dir} ne 'in_mem') {
        (undef, $filename) = tempfile(
            DIR     => $config{dir},
            UNLINK  => $config{unlink},
            EXLOCK  => 0,
        );
    }
    
    my $type    = $config{type} eq 'btree'                  ? $DB_BTREE
                : $config{type} eq 'hash'                   ? $DB_HASH
                : $config{type} eq 'recno'                  ? $DB_RECNO
                : croak "unknown DB type '$config{type}'"
                ;

    if ($config{func}) {
        $type->{'compare'} = $config{func};
    }

    my %h;
    my $db = tie %h, 'DB_File', $filename, undef, undef, $type
        or croak "cannot tie the file '$filename'";

    if ($config{multi}) {
        $db->filter_store_value( sub { $_ = freeze($_) } );
        $db->filter_fetch_value( sub { $_ = thaw($_) } );
    }

    return \%h;
}

1;

