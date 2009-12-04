#!/usr/bin/perl
# This is my perl conversion of scytale's levitation project
# (http://github.com/scy/levitation).

use feature ':5.10';

use strict;
use warnings;

use Coro;
use Coro::Channel;
use FindBin;
use lib "$FindBin::Bin";
use Inline 'C';

use Carp;

# don't overwrite CORE::length; thus only use -> no
use bytes;
no bytes;

use Regexp::Common qw(URI net);
use POSIX qw(strftime);
use List::Util qw(min first);
use Getopt::Long;
#use Storable qw(thaw freeze);
#use Digest::SHA1 qw(sha1);
use Time::Piece;
use JSON::XS;

use PrimitiveXML;

binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');


my $PAGES       = 10;
my $COMMITTER   = 'Levitation-pl <lev@servercare.eu>';
my $DEPTH       = 3;
my $DIR         = '.';
my $DB          = 'tc';
my $CURRENT;
my $HELP;
my $MAX_GFI     = 1000000;
my $GFI_CMD     = 'git fast-import --quiet';
my @NS;
my $ONE;

my $result = GetOptions(
    'max|m=i'       => \$PAGES,
    'depth|d=i'     => \$DEPTH,
    'tmpdir|t=s'    => \$DIR,
    'db=s'          => \$DB,
    'ns|n=s'        => \@NS,
    'current|c'     => \$CURRENT,
    'help|?'        => \$HELP,
    'one|o'         => \$ONE,
);
usage() if !$result || $HELP;

if ($DB eq 'tc') {
    eval { require TokyoCabinet; };
    if ($@) {
        print STDERR "cannot use TokyoCabinet, falling back to DB_File\nReason: $@\n";
        $DB = 'bdb';
    }
}
if ($DB eq 'bdb') {
    eval { require DB_File; };
    if ($@) {
        print STDERR "cannot use DB_File, terminating.\nReason: $@\n";
        $DB = undef;
    }
}
croak "couldn't load a persistent DB backend. Terminating." if not defined $DB;

# FIXME: would like to use Time::Piece's strftime(), but it returns the wrong timezone
my $TZ = $CURRENT ? strftime('%z', localtime()) : '+0000';

my $filename = "$DIR/levit.db";

my $stream = \*STDIN;

# put the parsing in a thread and provide a queue to give parses back through
my $queue = Coro::Channel->new(10000);
my $thr = Coro->new(\&thr_parse, $stream, $queue, $PAGES, \@NS);
$thr->ready;

my $CACHE = get_db($filename, 'new', $DB);

my $domain = $queue->get;
$domain = "git.$domain";

my $c_rev = 0;
my $max_id = 0;

open(my $gfi, '|-:utf8', $GFI_CMD) or croak "error opening pipe to 'git fast-import': $!";
while (defined(my $page = $queue->get) ) {
    # record current revision and page ids to be able to provide meaningful progress messages
    if ($page->{new}) {
        printf {$gfi} "progress processing page '%s:%s'  $c_rev / < $max_id\n", $page->{namespace}, $page->{title};
    }
    my $revid = $page->{revision_id};
    $max_id = $revid if $revid > $max_id;

    # and give the text to stdout, so git fast-import has something to do
    my $text = $page->{text} // "";
    my $len = bytes::length($text);

    print {$gfi} sprintf(qq{blob\ndata %d\n%s\n}, $len, $text);

    my $sha1 = '.' x 20;
    do {
        use bytes;
        my $stxt = sprintf(qq{blob %d\x00%s}, $len, $text);
        sha1($stxt, bytes::length($stxt), $sha1)
    };

    # extract all relevant data
    my %rev = (
        user    => user($page, $domain),
        comment => ($page->{comment} // "") . ( $page->{minor} ? " (minor)" : ""),
        timestamp    => $page->{timestamp},
        pid     => $page->{id},
        ns      => $page->{namespace},
        title   => $page->{title},
        sha1    => $sha1
    );

    # persist the serialized data with rev id as reference
    $CACHE->{"$revid"}= encode_json(\%rev);

    $c_rev++;
}
# we don't need the worker thread anymore. The input can go, too.
$thr->join();
close($stream);

untie %$CACHE;
undef %$CACHE;

exit(0) if $ONE;

$CACHE = get_db($filename, 'read', $DB);

# go over the persisted metadata with a cursor
say {$gfi} "progress processing $c_rev revisions";

my $commit_id = 1;
while (my ($revid, $fr) = each %$CACHE){
    if ($commit_id % 100000 == 0) {
        say {$gfi} "progress revision $commit_id / $c_rev";
    }

    my $rev = decode_json($fr);
    my $msg = "$rev->{comment}\n\nLevit.pl of page $rev->{pid} rev $revid\n";

    my @parts = ($rev->{ns});
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

    print {$gfi} sprintf(
q{commit refs/heads/master
author %s %s +0000
committer %s %s %s
data %d
%s
M 100644 %s %s
},
    $rev->{user}, $wtime, $COMMITTER, $ctime, $TZ, bytes::length($msg), $msg, unpack('H*', $rev->{sha1}), join('/', @parts));

    $commit_id++;
}

untie %$CACHE;
say {$gfi} "progress all done! let git fast-import finish ....";

close($gfi) or croak "error closing pipe to 'git fast-import': $!";

# get an author string that makes git happy and contains all relevant data
sub user {
    my ($page, $domain) = @_;

    my $uid = $page->{userid};
    my $uname = $page->{username} || $page->{userid};
    
    my $ip;
    if (!defined $uid || !defined $uname) {
        $ip = is_ipv4($page->{ip}) // '255.255.255.255';
    }
    my $email = defined $uid    ? sprintf("uid-%s@%s", $uid, $domain)
              : defined $ip     ? sprintf("ip-%s@%s", $ip, $domain)
              :                   sprintf("unknown@%s", $domain);

    $email = sprintf ("%s <%s>", $uname // $ip, $email);
    return $email;
}

sub is_ipv4 {
    my $value = shift;
 
    return unless defined($value);
 
    my(@octets) = $value =~ /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;
    return unless (@octets == 4);
    foreach (@octets) {
        return unless ($_ <= 255);
    }
 
    return join('.', @octets);
}
 
# open the wanted DB interface, with the desired mode, configure it
# and return a tied hash reference.
sub get_db {
    my ($filename, $mode, $option) = @_;


    if ($option eq 'tc') {
        return get_tc_db($filename, $mode);
    }
    elsif ($option eq 'bdb') {
        return get_bdb_db($filename, $mode);
    }
}

sub get_tc_db {
    my ($filename, $mode) = @_;

    
    my %t;
    if ($mode eq 'new') {
        # use TokyoCabinet BTree database as persistent storage
        my $c = "TokyoCabinet::BDB"->new()                                    or croak "cannot create new DB: $!";
        my $tflags = $c->TLARGE; #|$c->TDEFLATE;
        my $mflags = $c->OWRITER|$c->OCREAT|$c->OTRUNC;
        # sort keys as decimals
        $c->setcmpfunc($c->CMPDECIMAL)                                      or croak "cannot set function: $!";
        # use a large bucket
        $c->tune(128, 256, 3000000, 4, 10, $tflags)                         or croak "cannot tune DB: $!";
        $c->open($filename, $mflags)                                        or croak "cannot open DB: $!";
        $c->close()                                                         or croak "cannot close DB: $!";
        tie %t, "TokyoCabinet::BDB", $filename, "TokyoCabinet::BDB"->OWRITER()  or croak "cannot tie DB: $!";
    }
    elsif ($mode eq 'write') {
        tie %t, "TokyoCabinet::BDB", $filename, "TokyoCabinet::BDB"->OWRITER()  or croak "cannot tie DB: $!";
    }
    elsif ($mode eq 'read') {
        tie %t, "TokyoCabinet::BDB", $filename, "TokyoCabinet::BDB"->OREADER()   or croak "cannot tie DB: $!";
    }
    return \%t;
}

sub get_bdb_db {
    my ($filename, $mode) = @_;

    $DB_File::DB_BTREE->{compare} = sub { $_[0] <=> $_[1] };

    my %t;
    my $mflags;
    if ($mode eq 'new') {
        $mflags = DB_File::O_RDWR()|DB_File::O_TRUNC()|DB_File::O_CREAT();
    }
    elsif ($mode eq 'write') {
        $mflags = DB_File::O_RDWR();
    }
    elsif ($mode eq 'read') {
        $mflags = DB_File::O_RDONLY();
    }
    tie %t, 'DB_File', $filename, $mflags, undef, $DB_File::DB_BTREE    or croak "cannot open DB: $!";
    return \%t;
}

# parse the $stream and put the result to $queue
sub thr_parse {
    my ($stream, $queue, $MPAGES, $MNS) = @_;
    my $revs = PrimitiveXML->new(handle => $stream);

    # give the site's domain to the boss thread
    my (undef, undef, $domain) = ($revs->{base} =~ $RE{URI}{HTTP}{-keep});
    $queue->put($domain);
    
    my $c_page = 0;
    my $current = "";
    while (my $rev = $revs->next) {
        next if @$MNS && !first { $rev->{namespace} eq $_ } @$MNS;
        # more than max pages?
        if ($current ne $rev->{id}) {
            $current = $rev->{id};
            $c_page++;
            $rev->{new} = 1;
            last if $MPAGES > 0 && $c_page > $MPAGES;
        }

        $queue->put($rev);
    }

    # give an undef to boss thread, to signal "we are done"
    $queue->put(undef);
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

    -db (tc|bdb)    Define the database backend to use for persisting.
                    'tc' for Tokyo Cabinet is the default. 'bdb' is for
                    support via the standard Perl module DB_File;

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



__DATA__
__C__
/*
 * SHA1 routine optimized to do word accesses rather than byte accesses,
 * and to avoid unnecessary copies into the context array.
 *
 * This was initially based on the Mozilla SHA1 implementation, although
 * none of the original Mozilla code remains.
 */

/* this is only to get definitions for memcpy(), ntohl() and htonl() */
#include <string.h>

/*
 * SHA1 routine optimized to do word accesses rather than byte accesses,
 * and to avoid unnecessary copies into the context array.
 *
 * This was initially based on the Mozilla SHA1 implementation, although
 * none of the original Mozilla code remains.
 */

typedef struct {
	unsigned long long size;
	unsigned int H[5];
	unsigned int W[16];
} blk_SHA_CTX;

void blk_SHA1_Init(blk_SHA_CTX *ctx);
void blk_SHA1_Update(blk_SHA_CTX *ctx, const void *dataIn, unsigned long len);
void blk_SHA1_Final(unsigned char hashout[20], blk_SHA_CTX *ctx);

#define git_SHA_CTX	blk_SHA_CTX
#define git_SHA1_Init	blk_SHA1_Init
#define git_SHA1_Update	blk_SHA1_Update
#define git_SHA1_Final	blk_SHA1_Final

#if defined(__GNUC__) && (defined(__i386__) || defined(__x86_64__))

/*
 * Force usage of rol or ror by selecting the one with the smaller constant.
 * It _can_ generate slightly smaller code (a constant of 1 is special), but
 * perhaps more importantly it's possibly faster on any uarch that does a
 * rotate with a loop.
 */

#define SHA_ASM(op, x, n) ({ unsigned int __res; __asm__(op " %1,%0":"=r" (__res):"i" (n), "0" (x)); __res; })
#define SHA_ROL(x,n)	SHA_ASM("rol", x, n)
#define SHA_ROR(x,n)	SHA_ASM("ror", x, n)

#else

#define SHA_ROT(X,l,r)	(((X) << (l)) | ((X) >> (r)))
#define SHA_ROL(X,n)	SHA_ROT(X,n,32-(n))
#define SHA_ROR(X,n)	SHA_ROT(X,32-(n),n)

#endif

/*
 * If you have 32 registers or more, the compiler can (and should)
 * try to change the array[] accesses into registers. However, on
 * machines with less than ~25 registers, that won't really work,
 * and at least gcc will make an unholy mess of it.
 *
 * So to avoid that mess which just slows things down, we force
 * the stores to memory to actually happen (we might be better off
 * with a 'W(t)=(val);asm("":"+m" (W(t))' there instead, as
 * suggested by Artur Skawina - that will also make gcc unable to
 * try to do the silly "optimize away loads" part because it won't
 * see what the value will be).
 *
 * Ben Herrenschmidt reports that on PPC, the C version comes close
 * to the optimized asm with this (ie on PPC you don't want that
 * 'volatile', since there are lots of registers).
 *
 * On ARM we get the best code generation by forcing a full memory barrier
 * between each SHA_ROUND, otherwise gcc happily get wild with spilling and
 * the stack frame size simply explode and performance goes down the drain.
 */

#if defined(__i386__) || defined(__x86_64__)
  #define setW(x, val) (*(volatile unsigned int *)&W(x) = (val))
#elif defined(__GNUC__) && defined(__arm__)
  #define setW(x, val) do { W(x) = (val); __asm__("":::"memory"); } while (0)
#else
  #define setW(x, val) (W(x) = (val))
#endif

/*
 * Performance might be improved if the CPU architecture is OK with
 * unaligned 32-bit loads and a fast ntohl() is available.
 * Otherwise fall back to byte loads and shifts which is portable,
 * and is faster on architectures with memory alignment issues.
 */

#if defined(__i386__) || defined(__x86_64__) || \
    defined(__ppc__) || defined(__ppc64__) || \
    defined(__powerpc__) || defined(__powerpc64__) || \
    defined(__s390__) || defined(__s390x__)

#define get_be32(p)	ntohl(*(unsigned int *)(p))
#define put_be32(p, v)	do { *(unsigned int *)(p) = htonl(v); } while (0)

#else

#define get_be32(p)	( \
	(*((unsigned char *)(p) + 0) << 24) | \
	(*((unsigned char *)(p) + 1) << 16) | \
	(*((unsigned char *)(p) + 2) <<  8) | \
	(*((unsigned char *)(p) + 3) <<  0) )
#define put_be32(p, v)	do { \
	unsigned int __v = (v); \
	*((unsigned char *)(p) + 0) = __v >> 24; \
	*((unsigned char *)(p) + 1) = __v >> 16; \
	*((unsigned char *)(p) + 2) = __v >>  8; \
	*((unsigned char *)(p) + 3) = __v >>  0; } while (0)

#endif

/* This "rolls" over the 512-bit array */
#define W(x) (array[(x)&15])

/*
 * Where do we get the source from? The first 16 iterations get it from
 * the input data, the next mix it from the 512-bit array.
 */
#define SHA_SRC(t) get_be32(data + t)
#define SHA_MIX(t) SHA_ROL(W(t+13) ^ W(t+8) ^ W(t+2) ^ W(t), 1)

#define SHA_ROUND(t, input, fn, constant, A, B, C, D, E) do { \
	unsigned int TEMP = input(t); setW(t, TEMP); \
	E += TEMP + SHA_ROL(A,5) + (fn) + (constant); \
	B = SHA_ROR(B, 2); } while (0)

#define T_0_15(t, A, B, C, D, E)  SHA_ROUND(t, SHA_SRC, (((C^D)&B)^D) , 0x5a827999, A, B, C, D, E )
#define T_16_19(t, A, B, C, D, E) SHA_ROUND(t, SHA_MIX, (((C^D)&B)^D) , 0x5a827999, A, B, C, D, E )
#define T_20_39(t, A, B, C, D, E) SHA_ROUND(t, SHA_MIX, (B^C^D) , 0x6ed9eba1, A, B, C, D, E )
#define T_40_59(t, A, B, C, D, E) SHA_ROUND(t, SHA_MIX, ((B&C)+(D&(B^C))) , 0x8f1bbcdc, A, B, C, D, E )
#define T_60_79(t, A, B, C, D, E) SHA_ROUND(t, SHA_MIX, (B^C^D) ,  0xca62c1d6, A, B, C, D, E )

static void blk_SHA1_Block(blk_SHA_CTX *ctx, const unsigned int *data)
{
	unsigned int A,B,C,D,E;
	unsigned int array[16];

	A = ctx->H[0];
	B = ctx->H[1];
	C = ctx->H[2];
	D = ctx->H[3];
	E = ctx->H[4];

	/* Round 1 - iterations 0-16 take their input from 'data' */
	T_0_15( 0, A, B, C, D, E);
	T_0_15( 1, E, A, B, C, D);
	T_0_15( 2, D, E, A, B, C);
	T_0_15( 3, C, D, E, A, B);
	T_0_15( 4, B, C, D, E, A);
	T_0_15( 5, A, B, C, D, E);
	T_0_15( 6, E, A, B, C, D);
	T_0_15( 7, D, E, A, B, C);
	T_0_15( 8, C, D, E, A, B);
	T_0_15( 9, B, C, D, E, A);
	T_0_15(10, A, B, C, D, E);
	T_0_15(11, E, A, B, C, D);
	T_0_15(12, D, E, A, B, C);
	T_0_15(13, C, D, E, A, B);
	T_0_15(14, B, C, D, E, A);
	T_0_15(15, A, B, C, D, E);

	/* Round 1 - tail. Input from 512-bit mixing array */
	T_16_19(16, E, A, B, C, D);
	T_16_19(17, D, E, A, B, C);
	T_16_19(18, C, D, E, A, B);
	T_16_19(19, B, C, D, E, A);

	/* Round 2 */
	T_20_39(20, A, B, C, D, E);
	T_20_39(21, E, A, B, C, D);
	T_20_39(22, D, E, A, B, C);
	T_20_39(23, C, D, E, A, B);
	T_20_39(24, B, C, D, E, A);
	T_20_39(25, A, B, C, D, E);
	T_20_39(26, E, A, B, C, D);
	T_20_39(27, D, E, A, B, C);
	T_20_39(28, C, D, E, A, B);
	T_20_39(29, B, C, D, E, A);
	T_20_39(30, A, B, C, D, E);
	T_20_39(31, E, A, B, C, D);
	T_20_39(32, D, E, A, B, C);
	T_20_39(33, C, D, E, A, B);
	T_20_39(34, B, C, D, E, A);
	T_20_39(35, A, B, C, D, E);
	T_20_39(36, E, A, B, C, D);
	T_20_39(37, D, E, A, B, C);
	T_20_39(38, C, D, E, A, B);
	T_20_39(39, B, C, D, E, A);

	/* Round 3 */
	T_40_59(40, A, B, C, D, E);
	T_40_59(41, E, A, B, C, D);
	T_40_59(42, D, E, A, B, C);
	T_40_59(43, C, D, E, A, B);
	T_40_59(44, B, C, D, E, A);
	T_40_59(45, A, B, C, D, E);
	T_40_59(46, E, A, B, C, D);
	T_40_59(47, D, E, A, B, C);
	T_40_59(48, C, D, E, A, B);
	T_40_59(49, B, C, D, E, A);
	T_40_59(50, A, B, C, D, E);
	T_40_59(51, E, A, B, C, D);
	T_40_59(52, D, E, A, B, C);
	T_40_59(53, C, D, E, A, B);
	T_40_59(54, B, C, D, E, A);
	T_40_59(55, A, B, C, D, E);
	T_40_59(56, E, A, B, C, D);
	T_40_59(57, D, E, A, B, C);
	T_40_59(58, C, D, E, A, B);
	T_40_59(59, B, C, D, E, A);

	/* Round 4 */
	T_60_79(60, A, B, C, D, E);
	T_60_79(61, E, A, B, C, D);
	T_60_79(62, D, E, A, B, C);
	T_60_79(63, C, D, E, A, B);
	T_60_79(64, B, C, D, E, A);
	T_60_79(65, A, B, C, D, E);
	T_60_79(66, E, A, B, C, D);
	T_60_79(67, D, E, A, B, C);
	T_60_79(68, C, D, E, A, B);
	T_60_79(69, B, C, D, E, A);
	T_60_79(70, A, B, C, D, E);
	T_60_79(71, E, A, B, C, D);
	T_60_79(72, D, E, A, B, C);
	T_60_79(73, C, D, E, A, B);
	T_60_79(74, B, C, D, E, A);
	T_60_79(75, A, B, C, D, E);
	T_60_79(76, E, A, B, C, D);
	T_60_79(77, D, E, A, B, C);
	T_60_79(78, C, D, E, A, B);
	T_60_79(79, B, C, D, E, A);

	ctx->H[0] += A;
	ctx->H[1] += B;
	ctx->H[2] += C;
	ctx->H[3] += D;
	ctx->H[4] += E;
}

void blk_SHA1_Init(blk_SHA_CTX *ctx)
{
	ctx->size = 0;

	/* Initialize H with the magic constants (see FIPS180 for constants) */
	ctx->H[0] = 0x67452301;
	ctx->H[1] = 0xefcdab89;
	ctx->H[2] = 0x98badcfe;
	ctx->H[3] = 0x10325476;
	ctx->H[4] = 0xc3d2e1f0;
}

void blk_SHA1_Update(blk_SHA_CTX *ctx, const void *data, unsigned long len)
{
	int lenW = ctx->size & 63;

	ctx->size += len;

	/* Read the data into W and process blocks as they get full */
	if (lenW) {
		int left = 64 - lenW;
		if (len < left)
			left = len;
		memcpy(lenW + (char *)ctx->W, data, left);
		lenW = (lenW + left) & 63;
		len -= left;
		data = ((const char *)data + left);
		if (lenW)
			return;
		blk_SHA1_Block(ctx, ctx->W);
	}
	while (len >= 64) {
		blk_SHA1_Block(ctx, data);
		data = ((const char *)data + 64);
		len -= 64;
	}
	if (len)
		memcpy(ctx->W, data, len);
}

void blk_SHA1_Final(unsigned char hashout[20], blk_SHA_CTX *ctx)
{
	static const unsigned char pad[64] = { 0x80 };
	unsigned int padlen[2];
	int i;

	/* Pad with a binary 1 (ie 0x80), then zeroes, then length */
	padlen[0] = htonl(ctx->size >> 29);
	padlen[1] = htonl(ctx->size << 3);

	i = ctx->size & 63;
	blk_SHA1_Update(ctx, pad, 1+ (63 & (55 - i)));
	blk_SHA1_Update(ctx, padlen, 8);

	/* Output hash */
	for (i = 0; i < 5; i++)
		put_be32(hashout + i*4, ctx->H[i]);
}

unsigned char* sha1(unsigned char* data, unsigned long size, unsigned char* digeststr) {
    git_SHA_CTX c;

    git_SHA1_Init(&c);
    git_SHA1_Update(&c, data, size);
    git_SHA1_Final(digeststr, &c);

    return digeststr;
}
