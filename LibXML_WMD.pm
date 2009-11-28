package LibXML_WMD;

use strict;
use warnings;
use XML::LibXML::Reader;
use XML::LibXML::XPathContext;

sub new {
    my ($class) = shift @_;

    ##### Configuration section #####
    my $NS = { x => 'http://www.mediawiki.org/xml/export-0.4/' };

    my %defaults = ();
    my %DEFS = (
        '//x:revision/x:id'                     => 'revision_id',
        '//x:revision/x:comment'                => 'comment',
        '//x:revision/x:minor'                  => 'minor',
        '//x:revision/x:text'                   => 'text',
        '//x:revision/x:timestamp'              => 'timestamp',
        '//x:revision/x:contributor/x:id'       => 'userid',
        '//x:revision/x:contributor/x:username' => 'username',
        '//x:revision/x:contributor/x:ip'       => 'ip',
    );
    my %PPATS = (
        title   => XML::LibXML::Pattern->new('//x:page/x:title',    $NS),
        id      => XML::LibXML::Pattern->new('//x:page/x:id',       $NS),
        rev     => XML::LibXML::Pattern->new('//x:revision',        $NS),
    );
    my %PATS = map { $_ => XML::LibXML::Pattern->new($_, $NS) } keys %DEFS;

    my $pattern = XML::LibXML::Pattern->new(join(q{|}, '//x:revision','//x:page/x:id','//x:page/x:title', keys %DEFS), $NS);
    my $si_pattern = XML::LibXML::Pattern->new('//x:siteinfo', $NS);
    ##### Configuration end #####


    my $reader = XML::LibXML::Reader->new( @_ );
    my $st = $reader->nextPatternMatch($si_pattern);
    die "cannot find siteinfo section" if $st <= 0;

    my $si = $reader->copyCurrentNode(1);

    my $XPC = XML::LibXML::XPathContext->new;
    $XPC->registerNs(%$NS);
    $XPC->setContextNode($si);

    my %self = (
        PATS        => \%PATS,
        PPATS       => \%PPATS,
        DEFS        => \%DEFS,
        pattern     => $pattern,
        page        => {},
        list        => [],
        reader      => $reader,
        base        => $XPC->findvalue("x:base"),
        sitename    => $XPC->findvalue("x:sitename"),
        _namespaces => {map { $_->textContent => $_->findvalue('@key') } $XPC->findnodes("x:namespaces/x:namespace") },
    );
    $self{nsre} = join( q{|}, map { quotemeta($_) } keys %{$self{_namespaces}} );

    return bless \%self, $class;
}

sub next {
    my ($self) = @_;
    my $reader = $self->{reader};

    my %data = ();
    my %page = %{ $self->{page} };
    
    ELT:
    while ($reader->nextPatternMatch($self->{pattern}) > 0 ) {

        next ELT unless $reader->nodeType() == 1;

        if ( $reader->matchesPattern($self->{PPATS}{title}) ) {
            
            $reader->read;
            my $value = $reader->value;
            my ($ns, $title);

            if ($value =~ m/^($self->{nsre}):(.+)/) {
                ($ns, $title) = ($1, $2);
            }
            else {
                ($ns, $title) = ('Main', $value);
            }
            
            my %h = (title => $title, namespace => $ns, nsid => $self->{_namespaces}{$ns} || 0);
            if (!%page) {
                %page = %h;
            }
            $self->{page} = \%h;

        }
        elsif ( $reader->matchesPattern($self->{PPATS}{id}) ) {

            $reader->read;
            my $value = $reader->value;
            $page{id} ||= $value;
            $self->{page}{id} = $value;

        }
        elsif ( $reader->matchesPattern($self->{PPATS}{rev}) ) {

            if (%data) {
                last ELT;
                # print Dumper({%page, %data});
            }

        }
        else {
            IN_REV:
            while (my ($k, $v) = each %{ $self->{PATS} }) {
                if ($reader->matchesPattern($v)) {
                    $reader->read;
                    $data{ $self->{DEFS}{$k} } = $reader->value;
                    #print $reader->name, ": ", $reader->readInnerXml, "\n";
                    # reset 'each' iterator
                    keys %{ $self->{PATS} };
                    last IN_REV;
                }
            }
        }
    
    }
    if (%data) {
        push @{ $self->{list} }, {%page, %data};
    }

    if (@{ $self->{list} }) {
        return( shift @{ $self->{list} } );
    }

    $reader->close();
    return;
}
1;
