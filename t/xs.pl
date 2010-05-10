#!/usr/bin/perl

use Inline C => 'DATA';
my $a = "bla\0laber\n";
chomp $a;
test1($a);

__DATA__
__C__

void test1(SV* sv) {
    STRLEN len;
    unsigned int clen;
    STRLEN cur;
    char* text;

    text = SvPV(sv, len);
    clen = strlen(text);
    cur = SvCUR(sv);

    printf("strlen: %d, len: %d, cur: %d\n", clen, len, cur);
}
