package PrimitiveXML;

use strict;
use warnings;
use XML::Bare;
use Scalar::Util qw(openhandle);
use HTML::Entities;

my $e2c = {
    amp => '&',
    'lt' => '<',
    'gt' => '>',
    quot => q{"},
    apos => q{'},
};
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

    my $infotxt = do {local $/ = "</siteinfo>\n"; <$in>;};
    my $info = XML::Bare->new(text => $infotxt)->parse()->{mediawiki}->{siteinfo};

    my %self = (
        page        => {},
        list        => [],
        reader      => $in,
        base        => $info->{base}->{value},
        sitename    => $info->{sitename}->{value},
        _namespaces => {
            map {
                $_->{value}  // "" => $_->{key}->{value}
            } @{ $info->{namespaces}->{namespace} }
        },
    );
    $self{nsre} = join( q{|}, map { quotemeta($_) } keys %{$self{_namespaces}} );

    return bless \%self, $class;
}

sub next {
    my ($self) = @_;
    my $reader = $self->{reader};

    if (!eof($reader) && !@{$self->{list}}) {
        local $/ = "</revision>\n";
        my $c = 0;
        while (($c < 50) && (my $line = <$reader>)) {
            push @{$self->{list}}, $line;
            $c++;
        }
    }
    my $elt = shift @{$self->{list}};
    return if not $elt;

    #print STDERR "$elt\n";
    substr($elt, 0, 10) = '' if substr($elt, 0, 9) eq '  </page>';

    my $r;
    if (substr($elt,0,14) eq '    <revision>') {
        $r = (XML::Bare->new(text => $elt))[1]->{revision};
    }
    elsif (substr($elt,0,8) eq '  <page>') {
        my $p = XML::Bare->new(text => $elt)->parse;
        my $value = $p->{page}->{title}->{value} // "";
        _decode_entities($value, $e2c);
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
    else {
        return;
    }
    
    my $c = $r->{comment}->{value} // "";
    my $t = $r->{text}->{value} // "";
    my $u = $r->{contributor}->{username}->{value} // "";
    _decode_entities($c, $e2c);
    _decode_entities($t, $e2c);
    _decode_entities($u, $e2c);

    my %data = (
        %{ $self->{page} },
        revision_id => $r->{id}->{value},
        comment     => $c,
        text        => $t,
        timestamp   => $r->{timestamp}->{value},
        userid      => $r->{contributor}->{id}->{value},
        username    => $u,
        ip          => $r->{contributor}->{ip}->{value},
    );
    $data{minor} = 1 if exists $r->{minor};
    return \%data;

}
1;
