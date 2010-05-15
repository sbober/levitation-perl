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
use TokyoCabinet;
use constant {
    EQUAL   => 0,
    INSERT  => 1,
    REPLACE => 2
};


sub new {
    my ($class) = @_;
    my $self = {
        array => [],
        indices => [],
        sizes => [],
        latest => undef,
        inspos => undef,
        reppos => undef,
        changes => undef,
        oldlen => undef,
    };
    my $t = TokyoCabinet::ADB->new;
    $t->open('+');
    $self->{indices2} = $t->[0];
    $self->{obj} = $t;

    bless $self, $class;
}

sub add {
    my ($self, $elem) = @_;

    my $index = $self->{indices};
    my $list = $self->{array};
    my $sizes = $self->{sizes};

    die "noooo..." if !(@$list == @$index);

    my $part = $elem->[1];
    $part .= '/' if $elem->[0] eq '40000';
    my $itext = "$elem->[0] $elem->[1]\0$elem->[2]";
    my $ilen = bytes::length($itext);

    # use binary chop to find insert position
    my ($lo, $hi) = (0, $#{$index});
    while ($hi >= $lo) {
        my $mid     = int(($lo + $hi) / 2);
        my $mid_val = $index->[$mid];
        my $cmp     = $part cmp $mid_val;
        if ($cmp == 0) {
                $self->{oldlen} = $sizes->[$mid];
                $list->[$mid] = $itext;
                $index->[$mid] = $part;
                $sizes->[$mid] = $ilen;
                $self->{reppos} = $mid;
                $self->{changes}++;
                return;
        }
        elsif ($cmp > 0) {
                $lo = $mid + 1;
        }
        elsif ($cmp < 0) {
                $hi = $mid - 1;
        }
    }
    splice(@$list, $lo, 0, $itext);
    splice(@$index, $lo, 0, $part);
    splice(@$sizes, $lo, 0, $ilen);
    $self->{inspos} = $lo;
    $self->{changes}++;
}

sub add2 {
    my ($self, $elem) = @_;

    my $index = $self->{indices2};
    my $list = $self->{array};
    my $sizes = $self->{sizes};

    #die "noooo..." if @$list != @$index;

    my $part = $elem->[1];
    $part .= '/' if $elem->[0] eq '40000';
    my $itext = "$elem->[0] $elem->[1]\0$elem->[2]";
    my $ilen = bytes::length($itext);

    my $ind = TokyoCabinet::adb_get($index, $part);
    if (defined $ind) {
        $self->{oldlen} = $sizes->[$ind];
        $list->[$ind] = $itext;
        $sizes->[$ind] = $ilen;
        $self->{reppos} = $ind;
        $self->{changes}++;
    }
    else {
        use Data::Dump qw(dump);
        my $names = [];
        my $i = length($part);
        while (!@$names && $i >= 0) {
            $names = TokyoCabinet::adb_fwmkeys($index, substr($part, 0, $i), -1);
            $i--;
        }
#        say STDERR dump($names);
        # use binary chop to find insert position
        my ($lo, $hi) = (0, $#{$names});
        while ($hi >= $lo) {
            my $mid     = int(($lo + $hi) / 2);
            my $mid_val = $names->[$mid];
            my $cmp     = $part cmp $mid_val;
            $lo = $mid + 1 if $cmp > 0;
            $hi = $mid - 1 if $cmp < 0;
        }
#        say STDERR $lo;
        my $name = $names->[$lo];
        $lo = defined $name  ?  TokyoCabinet::adb_get($index, $name) : 
                      @$names ? TokyoCabinet::adb_get($index, $names->[-1]) + 1:
                                0;
        splice(@$list, $lo, 0, $itext);
        TokyoCabinet::adb_iterinit($index);
        while (defined(my $key = TokyoCabinet::adb_iternext($index))) {
            my $value = TokyoCabinet::adb_get($index, $key);
            $value++ if $value >= $lo;
            TokyoCabinet::adb_put($index, $key, $value);
        }
        TokyoCabinet::adb_put($index, $part, $lo);
        splice(@$sizes, $lo, 0, $ilen);
        $self->{inspos} = $lo;
        $self->{changes}++;
        #say STDERR dump((tied %$index)->size());
        
    }
}

sub get_object {
    my ($self) = @_;

    return join '', @{ $self->{array} };
}

sub get_diff {
    my ($self) =  @_;
    return if !$self->{changes};
    croak "more than one change" if $self->{changes} > 1;
    my $sizes = $self->{sizes};
    #croak "called with too small tree" if @$sizes < 2;
    my ($idx, $action) = defined $self->{inspos} ? ($self->{inspos}, INSERT)
                                                 : ($self->{reppos}, REPLACE);

    my $it = $sizes->[$idx];
    
    my @answer;
    my $presum = Faster::array_sum($sizes, 0, $idx-1);
    my $postsum = Faster::array_sum($sizes, $idx+1, scalar(@$sizes) - 1);

    if ($presum) {
        push @answer, [EQUAL, 0, $presum, 0, $presum];
    }

    if ($action == INSERT) {
        push @answer, [$action, $presum, $presum, $presum, $presum+$it];
        if ($postsum) {
            push @answer, [EQUAL, $presum, $presum+$postsum, $presum+$it, $presum+$postsum+$it];
        }
    }
    elsif ($action == REPLACE) {
        push @answer, [$action, $presum, $presum+$self->{oldlen}, $presum, $presum+$it];
        if ($postsum) {
            push @answer, [EQUAL, $presum+$self->{oldlen}, $presum+$self->{oldlen}+$postsum, $presum+$it, $presum+$postsum+$it];
        }
    }
    
    return \@answer;
}

sub reset {
    my ($self) = @_;
    $self->{inspos} = undef;
    $self->{reppos} = undef;
    $self->{oldlen} = undef;
    $self->{changes} = undef;

}
1;

=pod



=cut

