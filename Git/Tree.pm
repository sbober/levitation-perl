package Git::Tree;

=head1 NAME

Git::Tree - represent a git tree with a sorted list

=cut

use feature ':5.10';

use strict;
use warnings;
use Carp;

use List::Util qw(sum);
use Faster;

use constant {
    EQUAL   => 0,
    INSERT  => 1,
    REPLACE => 2
};


sub new {
    my ($class) = @_;
    my $self = {
        indices => [],
        indices2 => {'' => 0},
        sizes => [0],
        full => '',
        diff => undef,
    };
    bless $self, $class;
}


sub add {
    my ($self, $elem) = @_;

    my $index = $self->{indices2};
    my $sizes = $self->{sizes};

    my $part = $elem->[1];
    $part .= '/' if $elem->[0] eq '40000';
    my $itext = "$elem->[0] $elem->[1]\0$elem->[2]";
    my $ilen = bytes::length($itext);
    my @ret;
    if (exists $index->{$part}) {
        my $ind = $index->{$part};
        substr($self->{full}, $sizes->[$ind-1], $ilen, $itext);
        # TODO: diff calc
        
        my $presum = $sizes->[$ind-1];
        my $indsum = $sizes->[$ind];
        my $postsum = $sizes->[-1];
        if ($presum) {
            push @ret, [EQUAL, 0, $presum, 0, $presum];
        }
        push @ret, [REPLACE, $presum, $indsum, $presum, $indsum];
        if ($postsum > $indsum) {
            push @ret, [EQUAL, $indsum, $postsum, $indsum, $postsum];
        }

        $self->{diff} = \@ret;
    }
    else {
        my @names = sort keys %$index;
        # use binary chop to find insert position
        my ($lo, $hi) = (0, $#names);
        while ($hi >= $lo) {
            my $mid     = int(($lo + $hi) / 2);
            my $mid_val = $names[$mid];
            my $cmp     = $part cmp $mid_val;
            $lo = $mid + 1 if $cmp > 0;
            $hi = $mid - 1 if $cmp < 0;
        }
        substr($self->{full}, $sizes->[$lo-1], 0, $itext);
        for my $v (values %$index) { # remember: 'values' returns aliased values
            $v++ if $v >= $lo;
        }
        $index->{$part} = $lo;
        my $offset = $ilen + $sizes->[$lo-1];
        splice(@$sizes, $lo, 0, $offset);
        $_ += $ilen for @{$sizes}[$lo+1 .. @$sizes - 1]; 
        
        
        my $presum = $sizes->[$lo - 1];
        my $indsum = $sizes->[$lo];
        my $postsum = $sizes->[-1];

        if ($presum) {
            push @ret, [EQUAL, 0, $presum, 0, $presum];
        }
        push @ret, [INSERT, $presum, $presum, $presum, $indsum];
        if ($postsum > $indsum) {
            push @ret, [EQUAL, $presum, $postsum-$ilen, $indsum, $postsum];
        }

        $self->{diff} = \@ret;
    }
}



1;

=pod



=cut

