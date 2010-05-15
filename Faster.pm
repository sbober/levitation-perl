package Faster;

use Inline (
    C => 'DATA',
    CCFLAGS => '-O0',
    CC => 'gcc-4.3',
    LIBS => '-lz',
    FORCE_BUILD => 1
);
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

#include <zlib.h>

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

SV* sha1(SV* data) {
    unsigned char digest[20];
    unsigned long size;
    unsigned char* real = SvPV(data, size);
    git_SHA_CTX c;

    git_SHA1_Init(&c);
    git_SHA1_Update(&c, real, size);
    git_SHA1_Final(digest, &c);

    return newSVpvn(digest, 20);
}

SV* calc_hash(SV* type, SV* content) {
    unsigned char* type_str;
    unsigned char* content_str;
    STRLEN type_len;
    STRLEN content_len;
    unsigned char digest[20];
    int len = 0;
    git_SHA_CTX c;

    unsigned char hdr[64];

    type_str = SvPV(type, type_len);
    content_str = SvPV(content, content_len); 

    len = snprintf(hdr, sizeof(hdr), "%s %d\0", type_str, content_len);

    git_SHA1_Init(&c);
    git_SHA1_Update(&c, hdr, len+1);
    git_SHA1_Update(&c, content_str, content_len);
    git_SHA1_Final(digest, &c);

    return newSVpvn(digest, 20);
}

SV* encode_size(unsigned long size) {
    unsigned char c;
    unsigned char out[20];
    int n = 0;

    c = size & 0x7f;
    size >>= 7;
    while (size) {
        out[n++] = c | 0x80;
        c = size & 0x7f;
        size >>= 7;
    }
    out[n] = c;
    return sv_2mortal(newSVpvn(out,n+1));
}

SV* encode_size_with(SV* text) {
    unsigned long size;
    SvPV(text, size);
    return encode_size(size);
}

typedef enum {
    EQUAL = 0,
    INSERT = 1,
    REPLACE = 2
} dhunk_type;

SV* create_delta(unsigned long baselen, SV* target, AV* seq) {
    unsigned char out[4096];
    unsigned char *tmp;
    unsigned int n = 0;
    unsigned int x;

    SV *basesize, *targetsize;
    SV **toparray, **subarray;

    char *real;
    AV* av;
    
    real = SvPV_nolen(target); 
    basesize = encode_size(baselen);
    targetsize = encode_size_with(target);

    STRLEN len;

    tmp = SvPV(basesize, len);
    for (x = 0; x < len; x++) {
        out[n++] = tmp[x];
    }
    tmp = SvPV(targetsize, len);
    for (x = 0; x < len; x++) {
        out[n++] = tmp[x];
    }

    for (x = 0; x <= av_len(seq); x++) {
        int opcode;
        SV** subsv = av_fetch(seq, x, 0);
        AV* subav = (AV *) SvRV(*subsv);

        opcode = SvIV(*(av_fetch(subav, 0, 0)));
        if (opcode == EQUAL) {

            unsigned int i = 0, ofs = 0, end = 0, size = 0;
            unsigned char op = 0x80;
            unsigned char scratch[7];
            unsigned int n1 = 0;
            unsigned int z;

            ofs = SvIV(*(av_fetch(subav, 1, 0)));
            end = SvIV(*(av_fetch(subav, 2, 0)));
            size = end - ofs;

            for (i = 0; i <= 3; i++) {
                if (ofs & 0xff << i*8) {
                    scratch[n1++] = (ofs >> i*8) & 0xff;
                    op |= 1 << i;
                }
            }
            for (i = 0; i <= 2; i++) {
                if (size & 0xff << i*8) {
                    scratch[n1++] = (size >> i*8) & 0xff;
                    op |= 1 << (4+i);
                }
            }

            out[n++] = op;
            for (i = 0; i < n1; i++) {
                out[n++] = scratch[i];
            }
        }
        else if (opcode == INSERT || opcode == REPLACE) {
            unsigned int ofs=0, end=0, size=0, o=0;

            ofs = SvIV(*(av_fetch(subav, 3, 0)));
            end = SvIV(*(av_fetch(subav, 4, 0)));
            size = end - ofs;
            o = ofs;
            while (size > 127) {
                out[n++] = 127;
                memcpy(&out[n], real + o, 127);
                size -= 127;
                o += 127;
                n += 127;
            }
            out[n++] = (unsigned char) size;
            memcpy(&out[n], real + o, size);
            n += size;
        }
        
    }
    return newSVpvn(out,n);
    
}


unsigned long array_sum( AV* array, int start, int end) {
    unsigned int i;
    unsigned long sum = 0;

    if (start > end) return 0;

    for (i = start; i <= end; i++) {
        sum += SvIV(*(av_fetch(array, i, 0)));
    }
    return sum;
}


SV* deflate2( SV* in ) {
    void *out;
    z_stream s;
    STRLEN in_len;
    unsigned char* real;
    SV* ret;

    real = SvPV(in, in_len);
    memset(&s, 0, sizeof(s));

    deflateInit(&s, Z_DEFAULT_COMPRESSION);
    s.next_in = (void *)real;
    s.avail_in = in_len;
    s.avail_out = deflateBound(&s, s.avail_in);
    Newx(out, s.avail_out, char);
    s.next_out = out;
    while (deflate(&s, Z_FINISH) == Z_OK)
        /* Nothing */;
    deflateEnd(&s);

    ret = newSVpvn(out, s.total_out);
    Safefree(out);

    return ret;
}


void encode_packobj(int type, SV* content) {
    unsigned char* real;
    STRLEN clen;
    int n = 0;
    unsigned char hdr[10];
    unsigned char c;

    clen = SvCUR(content);

    c = (type << 4) | (clen & 15);
    clen >>= 4;

    while (clen) {
        hdr[n++] = c | 0x80;
        c = clen & 0x7f;
        clen >>= 7;
    }
    hdr[n] = c;

    Inline_Stack_Vars;
    Inline_Stack_Reset;
    Inline_Stack_Push(sv_2mortal(newSVpvn(hdr, n+1)));
    Inline_Stack_Push(sv_2mortal(deflate2(content)));
    Inline_Stack_Done;
}


SV* encode_ofs (unsigned long ofs) {
    unsigned char hdr[10];
    int pos = sizeof(hdr) - 1;

    hdr[pos] = ofs & 127;
    while (ofs >>= 7)
        hdr[--pos] = 128 | (--ofs & 127);

    return newSVpvn(hdr + pos, sizeof(hdr) - pos);
}

