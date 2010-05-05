package Git::Delta;

use strict;
use warnings;

use feature ':5.10';

use String::LCSS_XS qw(lcss);
sub new {
    my ($class, $a, $b) = @_;
    $a //= '';
    $b //= '';
    my $self = {};
    $self->{a} = $self->{b} = undef;
    my $out = bless $self, $class;
    $self->set_seqs($a, $b);
    return $out;
}
sub set_seqs {
    my ($self, $a, $b) = @_;

    $self->set_seq1($a);
    $self->set_seq2($b);
    #$self->__chain_b();
}

sub set_seq1 {
    my ($self, $a) = @_;

    return if $a eq ($self->{a} // '');
    $self->{a} = $a;
    $self->{matching_blocks} = $self->{opcodes} = undef;
}

sub set_seq2 {
    my ($self, $b) = @_;

    return if $b eq ($self->{b} // '');
    $self->{b} = $b;
    $self->{matching_blocks} = $self->{opcodes} = undef;
    $self->{fullbcount} = undef;
}

sub __chain_b {
    my ($self) = @_;
    my $b = $self->{b};
    my $n = length $b;
    my $b2j = {};
    $self->{b2j} = $b2j;
    my $populardict = {};
    for my $i (0..length($b)-1) {
        my $elt = substr($b, $i, 1);
        if (exists $b2j->{$elt}) {
            my $indices = $b2j->{$elt};
            if ($n >= 200 && @$indices * 100 > $n) {
                $populardict->{$elt} = 1;
                @$indices = ();
            }
            else {
                push @$indices, $i;
            }
        }
        else {
            $b2j->{$elt} = [$i];
        }
    }

    delete @{$b2j}{keys %$populardict};
}

sub flm {
    my ($self, $alo, $ahi, $blo, $bhi) = @_;

    my ($a, $b)  = ($self->{a}, $self->{b});

    my @res = lcss(substr($a, $alo, ($ahi-$alo)), substr($b, $blo, ($bhi-$blo)));
    if (@res) {
        return [$res[1]+$alo, $res[2]+$blo, length($res[0])];
    }
    else {
        return [0, 0, 0];
    }
}

sub flm2 {
    my ($self, $alo, $ahi, $blo, $bhi) = @_;

    my ($a, $b)  = ($self->{a}, $self->{b});
    use Tree::Suffix;
    my $t = Tree::Suffix->new(
        substr($a, $alo, ($ahi-$alo)), substr($b, $blo, ($bhi-$blo))
    );
    my $lcs = $t->lcs;
    if ($lcs) {
        my @res = $t->find($lcs);
        return [$res[0][1]+$alo, $res[1][1]+$blo, length($lcs)];
    }
    else {
        return [0, 0, 0];
    }
}
sub find_longest_match {
    my ($self, $alo, $ahi, $blo, $bhi) = @_;

    my ($a, $b, $b2j)  = ($self->{a}, $self->{b}, $self->{b2j});
    my ($besti, $bestj, $bestsize) = ($alo, $blo, 0);
    # find longest junk-free match
    # during an iteration of the loop, j2len[j] = length of longest
    # junk-free match ending with a[i-1] and b[j]
    my $j2len = {};
    my $nothing = [];
    for my $i ($alo..$ahi-1) {
        # look at all instances of a[i] in b; note that because
        # b2j has no junk keys, the loop is skipped if a[i] is junk
        my $newj2len = {};
        for my $j (@{ $b2j->{substr $a, $i, 1} // $nothing } ) {
            next if $j < $blo;
            last if $j >= $bhi;
            my $k = ($j2len->{$j-1} // 0 ) + 1;
            $newj2len->{$j} = $k;
            if ($k > $bestsize) {
                ($besti, $bestj, $bestsize) = ($i-$k+1, $j-$k+1, $k);
            }
        }
        $j2len = $newj2len;
    }

    while ($besti > $alo && $bestj > $blo &&
           substr($a, $besti-1, 1) eq substr($b, $bestj-1, 1)) {
           ($besti, $bestj, $bestsize) = ($besti-1, $bestj-1, $bestsize+1);
    }
    while ($besti + $bestsize < $ahi && $bestj + $bestsize < $bhi &&
            substr($a, $besti+$bestsize, 1) eq substr($b, $bestj+$bestsize, 1)) {
        $bestsize++;
    }

    # Now that we have a wholly interesting match (albeit possibly
    # empty!), we may as well suck up the matching junk on each
    # side of it too.  Can't think of a good reason not to, and it
    # saves post-processing the (possibly considerable) expense of
    # figuring out what to do with it.  In the case of an empty
    # interesting match, this is clearly the right thing to do,
    # because no other kind of match is possible in the regions.
    while ($besti > $alo && $bestj > $blo &&
           substr($a, $besti-1, 1) eq substr($b, $bestj-1, 1)) {
        ($besti, $bestj, $bestsize) = ($besti-1, $bestj-1, $bestsize+1);
    }
    while ($besti+$bestsize < $ahi && $bestj+$bestsize < $bhi &&
           substr($a, $besti+$bestsize, 1) eq substr($b, $bestj+$bestsize, 1)) {
        $bestsize++;
    }

    return [$besti, $bestj, $bestsize];
}

sub get_matching_blocks {
    my ($self) = @_;

    if (defined $self->{matching_blocks}) {
        return $self->{matching_blocks};
    }
    my ($la, $lb) = (length($self->{a}), length($self->{b}));

    # This is most naturally expressed as a recursive algorithm, but
    # at least one user bumped into extreme use cases that exceeded
    # the recursion limit on their box.  So, now we maintain a list
    # ('queue`) of blocks we still need to look at, and append partial
    # results to `matching_blocks` in a loop; the matches are sorted
    # at the end.
    my $queue = [[0, $la, 0, $lb]];
    my $matching_blocks = [];
    while (@$queue) {
        my ($alo, $ahi, $blo, $bhi) = @{ pop(@$queue) };
        #my $x = $self->find_longest_match($alo, $ahi, $blo, $bhi);
        my $x = $self->flm($alo, $ahi, $blo, $bhi);
        my ($i, $j, $k) = @$x;
        # a[alo:i] vs b[blo:j] unknown
        # a[i:i+k] same as b[j:j+k]
        # a[i+k:ahi] vs b[j+k:bhi] unknown
        if ($k) {   # if k is 0, there was no matching block
            push @$matching_blocks, $x;
            if ($alo < $i && $blo < $j) {
                push @$queue, [$alo, $i, $blo, $j];
            }
            if ($i+$k < $ahi && $j+$k < $bhi) {
                push @$queue, [$i+$k, $ahi, $j+$k, $bhi];
            }
        }
    }
    $matching_blocks = [sort { 
        $a->[0] <=> $b->[0]
                ||
        $a->[1] <=> $b->[1]
                ||
        $a->[2] <=> $b->[2]
    } @$matching_blocks];

    # It's possible that we have adjacent equal blocks in the
    # matching_blocks list now.  Starting with 2.5, this code was added
    # to collapse them.
    my ($i1, $j1, $k1) = (0,0,0);
    my $non_adjacent = [];
    for my $block (@$matching_blocks) {
        my ($i2, $j2, $k2) = @$block;
        if ($i1 + $k1 == $i2 && $j1 + $k1 == $j2) {
            $k1 += $k2;
        }
        else {
            if ($k1) {
                push @$non_adjacent, [$i1, $j1, $k1];
            }
            ($i1, $j1, $k1) = ($i2, $j2, $k2);
        }
    }
    if ($k1) {
        push @$non_adjacent, [$i1, $j1, $k1];
    }
    push @$non_adjacent, [$la, $lb, 0];
    $self->{matching_blocks} = $non_adjacent;
    return $non_adjacent;
}

sub get_opcodes {
    my ($self) = @_;

    return $self->{opcodes} if defined $self->{opcodes};
    my ($i, $j) = (0,0);
    my $answer = [];
    $self->{opcodes} = $answer;

    for my $block (@{ $self->get_matching_blocks() }) {
        my ($ai, $bj, $size) = @$block;
        my $tag = '';
        if ($i < $ai && $j < $bj) {
            $tag = 'replace';
        }
        elsif ($i < $ai) {
            $tag = 'delete';
        }
        elsif ($j < $bj) {
            $tag = 'insert';
        }

        if ($tag) {
            push @$answer, [$tag, $i, $ai, $j, $bj];
        }
        ($i, $j) = ($ai+$size, $bj+$size);
        if ($size) {
            push @$answer, ['equal', $ai, $i, $bj, $j];
        }
    }
    return $answer;
}
1;
