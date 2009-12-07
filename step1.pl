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
use Inline 'C';

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

    my $queue       = Thread::Queue->new();
    my $persister   = threads->create(\&persist, $queue);

    my $stream      = \*STDIN;

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

        blob_to_gfi( $gfi, $rev );

        sleep 1 while $queue->pending > 1000;
        $queue->enqueue($rev);

    }
    $queue->enqueue(undef);
    $persister->join;
    $parse->join;
    printf {$gfi} "progress step 1 done, written %d pages / %d revisions\n",
        $count_page, $count_rev;
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

    my $count = 0;
    while (my $data = $queue->dequeue) {

        my $dbnr = int($data->{revision_id} / 4000000);
        if (!exists $DB{"revs$dbnr"}) {
            $DB{"revs$dbnr"} = CDB_File->new(
                "$DIR/revs$dbnr.db", "t$dbnr.$$"
            )
                or croak "cannot create DB revs$dbnr: $!";
        }

        my $ip      = $data->{ip} // '';
        $ip         = inet_aton($ip) if $ip =~ /^[\d.]+$/;
        my $uid     = $data->{userid} // $ip // -1;
        my $isip    = defined $ip && ($uid eq $ip);

        my $sha1 = '.' x 20;
        do {
            use bytes;
            my $stxt = sprintf(qq{blob %d\x00%s}, $data->{len}, $data->{text});
            sha1($stxt, bytes::length($stxt), $sha1)
        };

        my $rev = encode_json([
            $uid, $isip, 
            @{$data}{qw/username page_id namespace title timestamp comment/},
            $sha1,
            defined($data->{minor})
        ]);
        $DB{"revs$dbnr"}->insert($data->{revision_id}, $rev);

        $count++;
    }

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


