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
    };
    my $out = bless $self, $class;
    $out->_mk_objcache;
    $out->_open;

    return $out;
}

sub _mk_objcache {
    my ($self) = @_;

    $self->{objcache} = {};
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

    my $ofs = $self->{lastofs} = sysseek($f, 0, 1); # emulate systell

    if ($prev_ofs) {
        $out .= Faster::encode_ofs($ofs - $prev_ofs);
    }
    $out .= $data;
    $self->{outbytes} += syswrite($f, $out);
    $self->{count}++;

    $self->{objcache}->{$hash} = "$ofs\0" . crc32($out);
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

    my $hash = Faster::calc_hash($type, $content);
    
    my $elem = $self->{objcache}->{$hash};
    if (!$elem) {
        $self->_write($hash, $type, $content);
    }
    else {
        $elem = [split "\0", $elem];
        $self->{lastofs} = $elem->[0];
    }

    return ($hash, $self->{lastofs});
}

sub delta_write {
    my ($self, $type, $content, $delta, $prev_ofs ) = @_;

    my $hash = Faster::calc_hash($type, $content);

    my $elem = $self->{objcache}->{$hash};
    if (!$elem) {
        $self->_write_delta($hash, $delta, $prev_ofs);
    }
    else {
        $elem = [split "\0", $elem];
        $self->{lastofs} = $elem->[0];
    }

    return ($hash, $self->{lastofs});
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
    #$self->{objdirect} = undef;
    $self->{objcache} = {};
    #$self->{objcache}->close();

    my $nameprefix = Git::Common::repo("objects/pack/pack-$res");
    unlink "$self->{filename}.map" if -e "$self->{filename}.map";
    rename( "$self->{filename}.pack", "${nameprefix}.pack" );
    rename( "$self->{filename}.idx", "${nameprefix}.idx" );

    return $res;
}

sub _write_idx {
    my ($self, $pack_sum) = @_;

    # create the pack id and populate fanout
    my $psum = Digest::SHA1->new;
    my %fanout;
    my $db = $self->{objcache};
    my @sorted = sort keys %$db;


    foreach my $k (@sorted) {
        $psum->add($k);
        $fanout{ord(substr($k, 0, 1))}++;

    };
    my $pack_id = unpack 'H*', $psum->digest;

    use Fcntl;

    sysopen my $fh, "$self->{filename}.idx", O_WRONLY|O_CREAT|O_TRUNC
        or die "cannot create .idx-file";
    binmode($fh);

    my $sum = Digest::SHA1->new;

    # write pack v2 header
    my $hdr = "\377tOc" . pack('L>', 2);
    $sum->add($hdr);
    syswrite($fh, $hdr);

    # generate fanout and write
    my $fout = join '', map {
        $fanout{$_+1} += $fanout{$_};
        pack('L>', $fanout{$_});
    } (0..255);

    $sum->add($fout);
    syswrite($fh, $fout);

    my $out = '';
    my $count = 0;
    # write SHA1s
    foreach my $k (@sorted) {
        $sum->add($k);
        $out .= $k;
        $count++;
        if ($count >= 65536) {
            syswrite($fh, $out);
            $count = 0;
            $out = '';
        }
    }
    if ($count) {
        syswrite($fh, $out);
        $count = 0;
        $out = '';
    }

    # write CRC32s
    foreach my $k (@sorted) {
        my $v = [split "\0", $db->{$k}];
        my $c = pack('L>', $v->[1] );
        $sum->add($c);

        $out .= $c;
        $count++;
        if ($count >= 65536) {
            syswrite($fh, $out);
            $count = 0;
            $out = '';
        }
    }
    if ($count) {
        syswrite($fh, $out);
        $count = 0;
        $out = '';
    }

    # write offsets
    foreach my $k (@sorted) {
        my $v = [split "\0", $db->{$k}];
        my $ofs = pack('L>', $v->[0] );
        $sum->add($ofs);

        $out .= $ofs;
        $count++;
        if ($count >= 65536) {
            syswrite($fh, $out);
            $count = 0;
            $out = '';
        }
    }
    if ($count) {
        syswrite($fh, $out);
        $count = 0;
        $out = '';
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
    $self->{lastofs} = undef;
#    $self->_mk_objcache;

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

1;
