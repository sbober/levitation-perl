package PrimitiveXML;

use strict;
use warnings;
use XML::Bare;
use Scalar::Util qw(openhandle);
use HTML::Entities;

sub new {
    my ($class) = shift @_;
    my ($method, $input) = @_;

    my $in;
    if ($method eq 'location') {
        open $in, '<', $input or die "cannot open input for method '$method'";
    }
    if ($method eq 'handle') {
        $in = openhandle($input) or die "cannot open input for method '$method'";

    }
    else {
        die "unsupported input method '$method'";
    }

    my $infotxt = do {local $/ = '</siteinfo>'; <$in>;};
    my $info = XML::Bare->new(text => $infotxt)->parse()->{mediawiki}->{siteinfo};

    my %self = (
        page        => {},
        list        => [],
        reader      => $in,
        base        => $info->{base}->{value},
        sitename    => $info->{sitename}->{value},
        _namespaces => {map {$_->{value} ||"" => $_->{key}->{value} } @{ $info->{namespaces}->{namespace} }  },
    );
    $self{nsre} = join( q{|}, map { quotemeta($_) } keys %{$self{_namespaces}} );

    return bless \%self, $class;
}

sub next {
    my ($self) = @_;
    my $reader = $self->{reader};

    my $elt = do { local $/ = '</revision>'; <$reader>; };
    return if not $elt;

    $elt =~ s{\A \s* </page>}{}xms;

    my $r;
    if ($elt =~ m{\A \s* <page>}xms) {
        my $p = XML::Bare->new(text => $elt)->parse;
        my $value = decode_entities($p->{page}->{title}->{value}||"");
        my ($ns, $title);

        if ($value =~ m/^($self->{nsre}):(.+)/) {
            ($ns, $title) = ($1, $2);
        }
        else {
            ($ns, $title) = ('Main', $value);
        }
        
        my $id = $p->{page}->{id}->{value};

        my %h = (title => $title, namespace => $ns, nsid => $self->{_namespaces}{$ns} || 0, id => $id);

        $self->{page} = \%h;
        $r = $p->{page}->{revision};
    }
    elsif ($elt =~ m{\A \s* <revision>}xms) {
        $r = XML::Bare->new(text => $elt)->parse()->{revision};
    }
    else {
        return;
    }
    

    my %data = (
        %{ $self->{page} },
        revision_id => $r->{id}->{value},
        comment     => decode_entities($r->{comment}->{value} || ""),
        text        => decode_entities($r->{text}->{value}||""),
        timestamp   => $r->{timestamp}->{value},
        userid      => $r->{contributor}->{id}->{value},
        username    => decode_entities($r->{contributor}->{username}->{value}||""),
        ip          => $r->{contributor}->{ip}->{value},
    );
    $data{minor} = 1 if exists $r->{minor};
    return \%data;

}
1;
