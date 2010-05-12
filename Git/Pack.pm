package Git::Pack;

use feature ':5.10';

use strict;
use warnings;

use Git::Tree;
use Git::Common qw(repo);
use Faster;
use File::Temp qw(tempfile);
use Carp::Assert;
use IPC::Cmd qw(run);
use Carp;
use Digest::SHA1;
use Compress::Raw::Zlib;
use bytes ();

my %_typemap = (
    commit      => 1,
    tree        => 2,
    blob        => 3,
    tag         => 4,
    'ofs-delta' => 6,
    'ref-delta' => 7
);
my %_typermap = (
    1 => 'commit',
    2 => 'tree',
    3 => 'blob',
    4 => 'tag',
    6 => 'ofs-delta',
    7 => 'ref-delta'
);

sub new {
    my ($class) = @_;
    my $self = {
        count => 0,
        outbytes => 0,
        filename => undef,
        file => undef,
        objcache => {}
    };

    my $out = bless $self, $class;

    $out->_open;

    return $out;
}

sub _open {
    my ($self) = @_;
    return if defined $self->{file};

    my ($fh, $name) = tempfile(
        SUFFIX => '.pack',
        DIR => Git::Common::repo('objects'),
        UNLINK => 0
    );
    binmode($fh, ':raw');
    $self->{file} = $fh;
    $self->{filename} = substr($name, 0, -5);
    syswrite($fh, "PACK\0\0\0\2\0\0\0\0");
}

sub _raw_write {
    my ($self, $hash, $out, $data, $prev_ofs) = @_;

    my $f = $self->{file};

    my $ofs = sysseek($f, 0, 1); # emulate systell

    if ($prev_ofs) {
        $out .= _encode_ofs($ofs - $prev_ofs);
    }
    $out .= $data;
    $self->{outbytes} += syswrite($f, $out);
    $self->{count}++;

    $self->{objcache}->{$hash} = [$ofs, crc32($out)];
}

sub _write {
    my ($self, $hash, $type, $content) = @_;
    $self->_raw_write($hash, Faster::encode_packobj($_typemap{$type}, $content) );
}

sub _write_delta {
    my ($self, $hash, $delta, $prev_ofs) = @_;

    my ($hdr, $data) = Faster::encode_packobj($_typemap{'ofs-delta'}, $delta);

    $self->_raw_write($hash, $hdr, $data, $prev_ofs);
}

sub maybe_write {
    my ($self, $type, $content) = @_;

    my $hash = calc_hash($type, $content);

    if (!exists $self->{objcache}->{$hash}) {
        $self->_write($hash, $type, $content);
    }

    return ($hash, $self->{objcache}->{$hash}->[0]);
}

sub delta_write {
    my ($self, $type, $content, $delta, $prev_ofs ) = @_;

    my $hash = calc_hash($type, $content);

    if (!exists $self->{objcache}->{$hash}) {
        $self->_write_delta($hash, $delta, $prev_ofs);
    }

    return ($hash, $self->{objcache}->{$hash}->[0]);
}


sub _end {
    my ($self) = @_;

    my $f = $self->{file};
    return if !defined $f;
    $self->{file} = undef;

    sysseek($f, 8, 0);
    my $cp = pack 'N', $self->{count};
    assert(bytes::length($cp) == 4);
    syswrite($f, $cp);

    sysseek($f, 0, 0);
    my $sum = Digest::SHA1->new;
    while (1) {
        my $b;
        my $s = sysread($f, $b, 65536);
        last if not $s;
        $sum->add($b);
    }
    my $pack_sum = $sum->digest;
    syswrite($f, $pack_sum);

    close($f);

    my $res = $self->_write_idx($pack_sum);
    chomp $res;
    print STDERR "PACKOUT: $res\n";
    $self->{objcache} = {};

    my $nameprefix = Git::Common::repo("objects/pack/pack-$res");
    unlink "$self->{filename}.map" if -e "$self->{filename}.map";
    rename( "$self->{filename}.pack", "${nameprefix}.pack" );
    rename( "$self->{filename}.idx", "${nameprefix}.idx" );

    return $res;
}

sub _write_idx {
    my ($self, $pack_sum) = @_;

    # create the pack id
    my @sorted = sort keys %{ $self->{objcache} };
    my $pack_id = unpack 'H*', Faster::sha1( join '', @sorted );

    use Fcntl;

    sysopen my $fh, "$self->{filename}.idx", O_WRONLY|O_CREAT|O_TRUNC
        or die "cannot create .idx-file";
    binmode($fh);

    my $sum = Digest::SHA1->new;

    # write pack v2 header
    my $hdr = "\377tOc" . pack('L>', 2);
    $sum->add($hdr);
    syswrite($fh, $hdr);

    # create and write fanout table
    my %fanout;
    for my $hash (@sorted) {
        $fanout{ord(substr($hash, 0, 1))}++;
    }
    my $fout = join '', map {
        $fanout{$_+1} += $fanout{$_};
        pack('L>', $fanout{$_});
    } (0..255);

    $sum->add($fout);
    syswrite($fh, $fout);

    # write SHA1s
    for my $hash (@sorted) {
        $sum->add($hash);
        syswrite($fh, $hash);
    }

    # write CRC32s
    for my $hash (@sorted) {
        my $c = pack('L>', $self->{objcache}->{$hash}->[1] );

        $sum->add($c);
        syswrite($fh, $c);
    }

    # write offsets
    for my $hash (@sorted) {
        my $ofs = pack('L>', $self->{objcache}->{$hash}->[0] );

        $sum->add($ofs);
        syswrite($fh, $ofs);
    }

    # write footer
    $sum->add($pack_sum);
    syswrite($fh, $pack_sum);
    syswrite($fh, $sum->digest);

    close($fh);
    return $pack_id;
}

sub close {
    my ($self) = @_;
    return $self->_end;
}

sub breakpoint {
    my ($self) = @_;
    my $id = $self->_end;
    $self->{outbytes} = 0;
    $self->{count} = 0;

    $self->_open;

    return $id;
}


sub calc_hash {
    my ($type, $content) = @_;
    my $header = sprintf "%s %d\0", $type, bytes::length($content);
    my $stxt = $header . $content;
    my $sum = Faster::sha1($stxt);
    return $sum;
}


sub encode_size {
    my ($size) = @_;
    my $out = '';
    my $c = $size & 0x7f;
    $size >>= 7;
    while ($size) {
        $out .= chr($c | 0x80);
        $c = $size & 0x7f;
        $size >>= 7;
    }
    $out .= chr($c);
    return $out;
}

sub create_delta {
    my ($baselen, $target, $seq) = @_;
    my $out = '';
    $out .= Faster::encode_size($baselen);
    $out .= Faster::encode_size_with($$target);

    foreach my $item (@$seq) {
        my ($opcode, $i1, $i2, $j1, $j2) = @$item;
        if ($opcode == Git::Tree::EQUAL) {
            my $scratch = '';
            my $op = 0x80;
            my $o = $i1;
            for my $i (0..3) {
                if ($o & 0xff << $i*8) {
                    $scratch .= chr(($o >> $i*8) & 0xff);
                    $op |= 1 << $i;
                }
            }
            my $s = $i2 - $i1;
            for my $i (0..2) {
                if ($s & 0xff << $i*8) {
                    $scratch .= chr(($s >> $i*8) & 0xff);
                    $op |= 1 << (4+$i);
                }
            }
            $out .= chr($op);
            $out .= $scratch;
        }
        if ($opcode == Git::Tree::REPLACE || $opcode == Git::Tree::INSERT) {
            my $s = $j2 - $j1;
            my $o = $j1;
            while ($s > 127) {
                $out .= chr(127);
                $out .= substr($$target, $o, 127);
                $s -= 127;
                $o += 127;
            }
            $out .= chr($s);
            $out .= substr($$target, $o, $s);
        }
    }
    return $out;

}

sub _encode_ofs {
    my ($ofs) = @_;

    my @a;
    my $pos = 0;
    $a[$pos] = $ofs & 127;
    while ($ofs >>= 7) { 
        $a[++$pos] = 128 | (--$ofs & 127)
    }
    my $out = join '', reverse( map { chr($_) } @a );
    return $out;
}


1;
