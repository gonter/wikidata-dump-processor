#!/usr/bin/perl

use strict;

use JSON;
use Data::Dumper;
$Data::Dumper::Indent= 1;

my $TSV_SEP= "\t";

my $fnm= '20141215.json';

my @langs= qw(en de it fr);

# statistics
my %types;
my %attrs;

# item statistics
my %lang_labels;
my %lang_descr;
my %lang_aliases;
my %prop_claims;
my %name_sitelinks;

my %props;

  my @item_attrs= qw(labels descriptions aliases claims sitelinks);

open (FI, '<:utf8', $fnm) or die;
my $line= 0;

open (FO, '>:utf8', 'items.csv') or die;
print FO join ($TSV_SEP, qw(line pos id type cnt_label cnt_desc cnt_aliases cnt_claims cnt_sitelink has_p625)), "\n";

open (DIAG, '>:utf8', '@diag') or die;

# Geo Coordinates
open (FO_P625, '>:utf8', 'p625.csv') or die;
print FO_P626 join ($TSV_SEP, qw(id pos geodata)), "\n";

<FI>;
my $pos;
LINE: while (1)
{
  $pos= tell(FI);
  my $l= <FI>;
  last unless (defined ($l));

  $line++;
  print $line, " ", $pos, "\n" if (($line % 100000) == 0);

  my $le= chop ($l);
  if ($l eq '[' || $l eq ']')
  {
    print "[$line] [$pos] skipping array bracket: $l\n";
    # $pos= tell(FI);
    next LINE;
  }

  my $sx= chop ($l); $l .= $sx if ($sx ne ',');

  # print "[$line] [$pos] [$l]\n";
  my $j;
  eval { $j= decode_json ($l); }; 
  if ($@)
  {
    print "[$line] [$pos] ERROR=[", $@, "]\n";
    print DIAG "[$line] [$pos] ERROR=[", $@, "] line=[$line]\n";
    # $pos= tell(FI);
    next LINE;
  }

  my ($id, $ty)= map { $j->{$_} } qw(id type);

  $types{$ty}++;

  if ($ty eq 'property')
  {
    # $pos= tell(FI);
    push (@{$props{$id}}, $j);
    next LINE;
  }

  if ($ty ne 'item')
  {
    print "[$line] [$pos] unknown type=[$ty]\n";
    print DIAG "[$line] [$pos] type=[$ty] line=[$line]\n";
    # $pos= tell(FI);
    next LINE;
  }

  foreach my $a (keys %$j) { $attrs{$a}++; }

  # grip and counts labels and descriptions
  my ($jl, $jd, $ja, $jc, $js)= map { $j->{$_} } @item_attrs;

  my $c_jl= counter ($jl, \%lang_labels);
  my $c_jd= counter ($jd, \%lang_descr);
  my $c_ja= counter ($ja, \%lang_aliases);
  my $c_jc= counter ($jc, \%prop_claims);
  my $c_js= counter ($js, \%name_sitelinks);

  my $has_p625= 0;
  if (exists ($jc->{'P625'}))
  {
    $has_p625= 1;

=begin comment

    'mainsnak' => {
      'property' => 'P625',
      'datatype' => 'globe-coordinate',
      'snaktype' => 'value',
      'datavalue' => {
        'value' => {
          'globe' => 'http://www.wikidata.org/entity/Q2', -- Earth!
          'precision' => '0.00027777777777778',
          'longitude' => '-73.563530555556',
          'latitude' => '45.510127777778',
          'altitude' => undef
        },
        'type' => 'globecoordinate'
      }
    },

=end comment
=cut

    my $x;
    eval { $x= $jc->{'P625'}->[0]->{'mainsnak'}->{'datavalue'}->{'value'} };
    if ($@ || !defined ($x))
    {
      print DIAG "$id x=[$x] e=[$@] P625=", Dumper ($jc->{'P625'});
    }
    else
    {
      $has_p625= 1;
      my $y= encode_json ($x);
      print FO_P625 join ($TSV_SEP, $id, $pos, $y), "\n";
    }
  }

# TODO: count claims, aliases, sitelinks, etc.

  my (%tlt_l, %tlt_d);
  foreach my $lang (@langs)
  {
    my $label= $jl->{$lang}->{'value'};
    my $desc=  $jd->{$lang}->{'value'};
    $tlt_l{$lang}= $label;
    $tlt_d{$lang}= $label;
  }

  # print "[$line] [$pos] ", Dumper ($j) if ($ty eq 'property');
  print FO join ($TSV_SEP, $line, $pos, $id, $ty, $c_jl, $c_jd, $c_ja, $c_jc, $c_js, $has_p625), "\n";

  # last if ($line >= 200000);
  # $pos= tell(FI);
}

# check if there are multiple definitions of the same property and flatten the structure a bit
open (PROPS_LIST, '>:utf8', 'props.csv') or die;
print PROPS_LIST join ($TSV_SEP, qw(prop def_cnt use_cnt datatype label_en descr_en)), "\n";
foreach my $prop (keys %props)
{
  my @prop= @{$props{$prop}};
  my $p0= $prop[0];
  if (@prop != 1)
  {
    print "ATTN: prop=[$prop] count=",(scalar @prop), "\n";
  }
  else
  {
    $props{$prop}= $p0;
  }

  my $dt= $p0->{'datatype'};
  my $l_en= $p0->{'labels'}->{'en'}->{'value'};
  my $d_en= $p0->{'descriptions'}->{'en'}->{'value'};
  print PROPS_LIST join ($TSV_SEP, $prop, (scalar @prop), $prop_claims{$prop}, $dt, $l_en, $d_en), "\n";
}
close (PROPS_LIST);

open (PROPS, '>:utf8', 'props.json') or die;
print PROPS encode_json (\%props);
close (PROPS);

print "lines: $line\n";
print "pos: $pos\n";
print "types: ", Dumper (\%types);
print "attrs: ", Dumper (\%attrs);
print "lang_labels: ", Dumper (\%lang_labels);
print "lang_descr: ", Dumper (\%lang_descr);
print "lang_aliases: ", Dumper (\%lang_aliases);
print "name_sitelinks: ", Dumper (\%name_sitelinks);
print "prop_claims: ", Dumper (\%prop_claims);

close(FI);

exit(0);

sub counter
{
  my $s= shift;
  my $a= shift;

  my @s= keys %$s; my $c_s= @s;
  foreach my $x (@s) { $a->{$x}++; }
  $c_s;
}

