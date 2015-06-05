#!/usr/bin/perl

use strict;

use JSON;
use Compress::Zlib;

use Data::Dumper;
$Data::Dumper::Indent= 1;

my $TSV_SEP= "\t";
my $OUT_CHUNK_SIZE= 500_000_000;
my $MAX_INPUT_LINES= undef;
# my $MAX_INPUT_LINES= 100_000;

# my $fnm= '20141215.json';
my $fnm= 'dumps/20150601.json.gz';

my @langs= qw(en de it fr);

my @PARS= ();
while (my $arg= shift (@ARGV))
{
  if ($arg eq '--') { push (@PARS, @ARGV); @ARGV=(); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($an, $av)= split ($1, '=', 2);
    usage();
  }
  elsif ($arg =~ /^-(.+)/)
  {
    foreach my $flag (split('', $1))
    {
      usage();
    }
  }
  else { push (@PARS, $arg); }
}

sub usage
{
  system ('perldoc', $0);
  exit;
}

if (@PARS)
{
  $fnm= shift (@PARS);
}

usage() unless (defined ($fnm));

analyze_dump ($fnm);

exit(0);

sub analyze_dump
{
  my $fnm= shift;

open (DIAG, '>:utf8', '@diag') or die;

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

if ($fnm =~ /\.gz$/)
{
  open (FI, '-|', "gunzip -c '$fnm'") or die "can't gunzip [$fnm]";
}
else
{
  open (FI, '<:utf8', $fnm) or die "can't read [$fnm]";
}
my $line= 0;

# item list
my $fnm_items= 'items.csv';

local *FO_ITEMS;
open (FO_ITEMS, '>:utf8', $fnm_items) or die "can't write to [$fnm_items]";
my @cols1= qw(line pos fo_count fo_pos_beg fo_pos_end id type cnt_label cnt_desc cnt_aliases cnt_claims cnt_sitelink);
print FO_ITEMS join ($TSV_SEP, @cols1, qw(has_props)), "\n";

# properties
my @cols_filt= (@cols1, 'lang', 'label', 'val');

sub wdpf
{
  my $prop= shift;
  my $label= shift;

  return new WikiData::Property::Filter ('property' => $prop, 'label' => $label , 'cols' => \@cols_filt);
}

my %filters=
(
  # person identifiers
  'P227'  => wdpf ( 'P227', 'GND identifier'),
  'P214'  => wdpf ( 'P214', 'VIAF identifier'),
  'P496'  => wdpf ( 'P496', 'ORCID identifier'),

  'P213'  => wdpf ( 'P213', 'ISNI'), # check

  # personal data?
  'P569'  => wdpf ( 'P569', 'Date of birth'),
  'P570'  => wdpf ( 'P570', 'Date of death'),

  # publications
  'P345'  => wdpf ( 'P345', 'IMDb identifier'),
  'P212'  => wdpf ( 'P212', 'ISBN-13'),
  'P236'  => wdpf ( 'P212', 'ISSN'),
  'P957'  => wdpf ( 'P957', 'ISBN-10'),

  'P356'  => wdpf ( 'P356', 'DOI'),
  'P1184' => wdpf ( 'P1184', 'Handle'),

  # MusicBrainz
  'P434'  => wdpf ( 'P434', 'MusicBrainz artist id'),
  'P435'  => wdpf ( 'P435', 'MusicBrainz work id'),
  'P436'  => wdpf ( 'P436', 'MusicBrainz release group id'),
  'P1004' => wdpf ( 'P1004', 'MusicBrainz place id'),

  'P625'  => wdpf ( 'P625', 'Geo Coordinates'),
);
my @filters= sort keys %filters;

# BEGIN output transcription
local *FO_RECODED;
my $fo_open= 0;
my $fo_count= 0;
my $fo_pos= 0;
my $fo_compress= 2;

sub close_fo
{
  if ($fo_open)
  {
    # print FO_RECODED "]\n";
    close (FO_RECODED);
    $fo_open= 0;
  }
}

sub open_fo
{
  close_fo();

  my $fo_fnm;

  if ($fo_compress == 1)
  {
    $fo_fnm= sprintf ("out/wdq%05d.gz", ++$fo_count);
    open (FO_RECODED, '|-', "gzip -c >'$fo_fnm'") or die "can't write to [$fo_fnm]";
  }
  elsif ($fo_compress == 2)
  {
    $fo_fnm= sprintf ("out/wdq%05d.cmp", ++$fo_count);
    open (FO_RECODED, '>:raw', $fo_fnm) or die "can't write to [$fo_fnm]";
  }
  else
  {
    $fo_fnm= sprintf ("out/wdq%05d", ++$fo_count);
    open (FO_RECODED, '>:utf8', $fo_fnm) or die "can't write to [$fo_fnm]";
  }

  $fo_open= 1;

  print "writing dumps to $fo_fnm\n";
  # print FO_RECODED "[\n";
  $fo_pos= tell (FO_RECODED);
}
# END output transcription

open_fo();

<FI>;
my $pos;
LINE: while (1)
{
  $pos= tell(FI);
  my $l= <FI>;
  last unless (defined ($l));

  if ($fo_pos >= $OUT_CHUNK_SIZE)
  {
    open_fo();
  }
  $fo_pos= tell(FO_RECODED);

  $line++;
  print join (' ', $line, $pos, $fo_count, $fo_pos), "\n" if (($line % 10_000) == 0);

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

  # my $py= substr($l, 0, 30) . '...' . substr ($l, -30);
  my $px;
  if ($fo_compress == 2)
  {
    $px= print FO_RECODED compress($l);
  }
  else
  {
    $px= print FO_RECODED $l, "\n";
  }
  my $fo_pos_end= tell (FO_RECODED);
  # print "px=[$px] l=[$py]\n";

  foreach my $a (keys %$j) { $attrs{$a}++; }

  # grip and counts labels and descriptions
  my ($jl, $jd, $ja, $jc, $js)= map { $j->{$_} } @item_attrs;

  my $c_jl= counter ($jl, \%lang_labels);
  my $c_jd= counter ($jd, \%lang_descr);
  my $c_ja= counter ($ja, \%lang_aliases);
  my $c_jc= counter ($jc, \%prop_claims);
  my $c_js= counter ($js, \%name_sitelinks);

  # language translations
  my (%tlt_l, %tlt_d);
  my ($pref_l, $lang_l);
  foreach my $lang (@langs)
  {
    my $label= $jl->{$lang}->{'value'};
    my $desc=  $jd->{$lang}->{'value'};
    $tlt_l{$lang}= $label;
    $tlt_d{$lang}= $label;

    unless (defined ($pref_l))
    {
      $pref_l= $label;
      $lang_l= $lang;
    }
  }
  # print "tlt_l: ", Dumper (\%tlt_l);
  # print "tlt_d: ", Dumper (\%tlt_d);

  my @found_properties= ();
  foreach my $filtered_property (@filters)
  {
    if (exists ($jc->{$filtered_property}))
    {
      my $fp= $filters{$filtered_property};
      # print "fp: ", Dumper ($fp);
      my $p= $jc->{$filtered_property};
      # print "p: ", Dumper ($p);

      my $x;
      eval { $x= $p->[0]->{'mainsnak'}->{'datavalue'}->{'value'} };

      # print "x: ", Dumper ($x); # exit;

      if ($@)
      {
        print DIAG "id=$id error: filtered_property=[$filtered_property] $x=[$x] e=[$@] property=", Dumper ($p);
      }
      elsif (!defined ($x))
      {
        print DIAG "id=$id undef x: filtered_property=[$filtered_property] property=", Dumper ($p);
      }
      else
      {
  # ZZZ
        push (@found_properties, $filtered_property);

        my $y= (ref ($x) eq 'HASH') ? encode_json ($x) : $x;
        local *FO_p= $fp->{'_FO'};
        print FO_p join ($TSV_SEP,
                 $line, $pos, $fo_count, $fo_pos, $fo_pos_end,
                 $id, $ty,
                 $c_jl, $c_jd, $c_ja, $c_jc, $c_js,     # counters
                 $lang_l, $pref_l,
                 $y,
                 ), "\n";
      }
    }
  }

# TODO: count claims, aliases, sitelinks, etc.

  # print "[$line] [$pos] ", Dumper ($j) if ($ty eq 'property');
  print FO_ITEMS join ($TSV_SEP,
                 $line, $pos, $fo_count, $fo_pos, $fo_pos_end,
                 $id, $ty,
                 $c_jl, $c_jd, $c_ja, $c_jc, $c_js,     # counters
                 join (',', @found_properties)),
                 "\n";

  last if (defined ($MAX_INPUT_LINES) && $line >= $MAX_INPUT_LINES); ### DEBUG
  # $pos= tell(FI);
}

close (FI);
close_fo();

# check if there are multiple definitions of the same property and flatten the structure a bit
open (PROPS_LIST, '>:utf8', 'props.csv') or die;
print PROPS_LIST join ($TSV_SEP, qw(prop def_cnt use_cnt datatype label_en descr_en)), "\n";
foreach my $prop (sort keys %props)
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

print "pos: $pos\n";
print "types: ", Dumper (\%types);
print "attrs: ", Dumper (\%attrs);
print "lang_labels: ", Dumper (\%lang_labels);
print "lang_descr: ", Dumper (\%lang_descr);
print "lang_aliases: ", Dumper (\%lang_aliases);
print "name_sitelinks: ", Dumper (\%name_sitelinks);
print "prop_claims: ", Dumper (\%prop_claims);

print "lines: $line\n";
print "fo_count: $fo_count\n";

close(FI);

}

sub counter
{
  my $s= shift;
  my $a= shift;

  my @s= keys %$s; my $c_s= @s;
  foreach my $x (@s) { $a->{$x}++; }
  $c_s;
}

package WikiData::Property::Filter;

sub new
{
  my $class= shift;
  my %par= @_;

  my $obj= {};
  bless $obj, $class;
  $obj->set (%par);
  $obj->setup();

  $obj;
}

sub set
{
  my $obj= shift;
  my %par= @_;

  foreach my $par (keys %par)
  {
    $obj->{$par}= $par{$par};
  }
}

sub setup
{
  my $obj= shift;

  my ($property, $cols, $label)= map { $obj->{$_} } qw(property cols label); 
  my $res= undef;

      if ($property =~ m#^P\d+$#)
      {
        local *FO_Prop;
        my $fnm_prop= $property . '.csv';
        if (open (FO_Prop, '>:utf8', $fnm_prop))
        {
          print FO_Prop join ($TSV_SEP, @$cols), "\n" if (defined ($cols));
          print "writing filter [$property] [$label] to [$fnm_prop]\n";
          $obj->{'_FO'}= *FO_Prop;
          $res= 1;
        }
        else
        {
          print "can not write to [$fnm_prop]\n";
        }
      }
      else
      {
        print "ATTN: invalid property format [$property]; ignored\n";
        $res= -1
      }

  $res;
}

__END__
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

