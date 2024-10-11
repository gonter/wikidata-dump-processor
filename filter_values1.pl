#!/usr/bin/perl

=head1 NAME

 filter_values1.pl

=head1 DESCRIPTIONS

 filter values from exported Wikidata ndjson file

=head1 BUGS

 - find a better name
 - wanted property name is currently hard coded (see below)

=cut

use strict;

use lib 'lib';

use FileHandle;

use JSON;
use Compress::Zlib;

use Util::Simple_CSV;
use Util::hexdump;

use Data::Dumper;
$Data::Dumper::Indent= 1;
$Data::Dumper::Sort= 1;

use utf8;
use FileHandle;

binmode( STDOUT, ':utf8' ); autoflush STDOUT 1;
binmode( STDERR, ':utf8' ); autoflush STDERR 1;
binmode( STDIN,  ':utf8' );

my $wanted= 'P3097';

print join("\t", qw(id label values)), "\n";

while (my $j= <>)
{
  chop($j);
  my $d= JSON::decode_json($j);
  # print __LINE__, " d: ", Dumper($d);

  my ($id, $claims, $labels)= map { $d->{$_} } qw(title claims labels);
  # print __LINE__, " id=[$id] labels:", Dumper($labels);

  # my %labels= map { $_ => $labels->{value} } keys %$labels;
  my $label;
  foreach my $lang (qw(de en fr it cn), sort keys %$labels) { if (exists($labels->{$lang})) { $label= $labels->{$lang}->{value}; last; } }

  # print __LINE__, " id=[$id] label=[$label] claims:", Dumper($claims);
  my $xclaims= $claims->{$wanted};
  # print __LINE__, " id=[$id] label=[$label] wanted=[$wanted] xclaims:", Dumper($xclaims);

  my @values= map { my $ms= $_->{mainsnak}; my $dv= $ms->{datavalue}; ($ms->{snaktype} eq 'value') ? $dv->{value} : $ms->{snaktype} } @$xclaims;
  print join("\t", $id, $label, join(' ', @values)), "\n";

}

