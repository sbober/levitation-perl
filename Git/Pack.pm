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
    $self->_raw_write($hash, _encode_packobj($type, $content) );
}

sub _write_delta {
    my ($self, $hash, $delta, $prev_ofs) = @_;

    my ($hdr, $data) = _encode_packobj('ofs-delta', $delta);

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
    $self->{objcache} = {};

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
    syswrite($f, $sum->digest);

    close($f);

#    my @res = run(command => ['git', 'index-pack', '-v', '--index-version=2',
#                              "$self->{filename}.pack"]);
#    croak "error executing git index-pack: $res[1] | " . join('', @{$res[4]}) if !$res[0];
#    croak "git index-pack produced no output2" if !@{$res[3]};

#    my $out = join('', @{$res[3]});
    my $res = `git index-pack -v --index-version=2 $self->{filename}.pack`;
    chomp $res;
    croak "git index-pack produced no output2" if !$res;
    print STDERR "PACKOUT: $res\n";

    my $nameprefix = Git::Common::repo("objects/pack/pack-$res");
    unlink "$self->{filename}.map" if -e "$self->{filename}.map";
    rename( "$self->{filename}.pack", "${nameprefix}.pack" );
    rename( "$self->{filename}.idx", "${nameprefix}.idx" );

    return $res;
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

sub _encode_packobj {
    my ($type, $content) = @_;

    my $szout = '';
    my $sz = bytes::length($content);
    my $szbits = ($sz & 0x0f) | ($_typemap{$type} << 4);
    $sz >>= 4;
    
    while (1) {
        $szbits |= 0x80 if $sz;
        $szout .= chr($szbits);

        last if not $sz;

        $szbits = $sz & 0x7f;
        $sz >>= 7;
    }
    my $z = deflate($content);
    return ($szout, $z);
}

sub deflate {
    my ($t) = @_;
    my ($out1, $out2);
    my $err;
    state $x = Compress::Raw::Zlib::_deflateInit(
        0,
        Z_DEFAULT_COMPRESSION(),
        Z_DEFLATED(),
        MAX_WBITS(),
        MAX_MEM_LEVEL(),
        Z_DEFAULT_STRATEGY(),
        4096, ""
    );

    $err = $x->deflate($t, $out1);
    $err == Z_OK or die "cannot deflate object";
    
    $err = $x->flush($out2);
    $err == Z_OK or die "cannot finish object";

    $err = $x->deflateReset();
    $err == Z_OK or die "cannot reset object";

    return $out1 . $out2;
}
1;
