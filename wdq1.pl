#!/usr/bin/perl

use strict;

use JSON;

use Data::Dumper;
$Data::Dumper::Indent= 1;
use FileHandle;

use lib 'lib';
use WikiData::Utils;
use WikiData::Property::Filter;

use FDS;

my $TSV_SEP= "\t";
# my $OUT_CHUNK_SIZE= 500_000_000; # size of files containing item data in JSON format
my $OUT_CHUNK_SIZE= 640_000_000; # size of files containing item data in JSON format
my $MAX_INPUT_LINES= undef;
# my $MAX_INPUT_LINES= 100_000; # for debugging to limit processing time; TODO: add commandline option

my $exp_bitmap= 0; # 1..does not work; 2..makes no sense, too sparsely populated arrays
# not used my $LR_max_propid= 1930; # dump from 20150608

my $seq= 'a';
my $date= '2016-08-16'; # maybe a config file is in order to set up the defaults...
my ($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq);
my $upd_paths= 0;

my @langs= qw(en de it fr);

my $fo_compress= 2;
# 0..don't compress at all
# 1..compress output stream by piping into gzip; DO NOT USE
# 2..compress individual records using Compress::Zlib::compress()

my @PARS= ();
while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PARS, @ARGV); @ARGV=(); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($an, $av)= split ('=', $1, 2);
    print "an=[$an] av=[$av]\n";

       if ($an eq 'date') { $date= $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'seq')  { $seq=  $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'max-lines') { $MAX_INPUT_LINES=  $av || shift (@ARGV); }
    else
    {
      usage();
    }
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

($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq) if ($upd_paths);

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

my $e_start= time();
my $ts_start= localtime ($e_start);

print <<"EOX";
WikiData processor

date: $date
dump file name: $fnm
data dir: $data_dir
item dir: $out_dir
start_time: $ts_start
-----------
EOX

analyze_wikidata_dump ($fnm);

exit(0);

sub analyze_wikidata_dump
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

# local *FI= wkutils::open_input($fnm);
if ($fnm =~ /\.gz$/)
{
  open (FI, '-|', "gunzip -c '$fnm'") or die "can't gunzip [$fnm]";
}
# elsif bunzip ... see wkt1
else
{
  open (FI, '<:utf8', $fnm) or die "can't read [$fnm]";
}

my $line= 0;
my $t_start= time();

unless (-d $data_dir)
{
  print "mkdir $data_dir\n";
  mkdir ($data_dir);
}
unless (-d $out_dir)
{
  print "mkdir $out_dir\n";
  mkdir ($out_dir)
}

# item list
my $fnm_items= $data_dir . '/items.csv';

local *FO_ITEMS;
open (FO_ITEMS, '>:utf8', $fnm_items) or die "can't write to [$fnm_items]";
my @cols1= qw(line pos fo_count fo_pos_beg fo_pos_end id type cnt_label cnt_desc cnt_aliases cnt_claims cnt_sitelink lang label);
print FO_ITEMS join ($TSV_SEP, @cols1, qw(filtered_props claims)), "\n";
autoflush FO_ITEMS 1;

# properties
my @cols_filt= (@cols1, 'val');

sub wdpf
{
  my $prop= shift;
  my $label= shift;
  my $transform= shift;

  my $fnm_prop= $data_dir . '/' . $prop . '.csv';

  return new WikiData::Property::Filter ('property' => $prop, 'label' => $label , 'cols' => \@cols_filt, 'transform' => $transform, 'filename' => $fnm_prop);
}

my %filters=
(
  # structure
  'P31'  => wdpf ('P31', 'instance of', 1),
  'P279'  => wdpf ('P279', 'subclass of', 1),
  'P360'  => wdpf ('P360', 'is a list of', 1),
  'P361'  => wdpf ('P361', 'part of', 1),
  'P1269' => wdpf ('P1269', 'facet of', 1),

  # person identifiers
  'P227'  => wdpf ('P227', 'GND identifier'),
  'P214'  => wdpf ('P214', 'VIAF identifier'),
  'P496'  => wdpf ('P496', 'ORCID identifier'),

  'P213'  => wdpf ('P213', 'ISNI'), # check

  # personal data?
  'P569'  => wdpf ('P569', 'Date of birth'),
  'P570'  => wdpf ('P570', 'Date of death'),
  'P2298' => wdpf ('P2298', 'NSDAP membership number (1925-1945)'),

  # publications
  'P345'  => wdpf ('P345', 'IMDb identifier'),
  'P212'  => wdpf ('P212', 'ISBN-13'),
  'P236'  => wdpf ('P212', 'ISSN'),
  'P957'  => wdpf ('P957', 'ISBN-10'),

  # arXiv.org
  'P818'  => wdpf ('P818', 'arXiv ID'),
  'P820'  => wdpf ('P820', 'arXiv classification'),

  # permanent identifiers
  'P356'  => wdpf ('P356',  'DOI'),
  'P1184' => wdpf ('P1184', 'Handle'),
  'P727'  => wdpf ('P727',  'Europeana ID'),
  'P1036' => wdpf ('P1036', 'Dewey Decimal Classification'),
  'P563'  => wdpf ('P563',  'ICD-O'),

  'P1709' => wdpf ('P1709', 'equivalent class'),

  # Getty
  'P245'  => wdpf ('P245',  'ULAN identifier'), # Getty Union List of Artist Names
  'P1014' => wdpf ('P1014', 'AAT identifier'),  # Art & Architecture Thesaurus by the Getty Research Institute
  'P1667' => wdpf ('P1667', 'TGN identifier'),  # Getty Thesaurus of Geographic Names
  'P2432' => wdpf ('P2432', 'J. Paul Getty Museum artist id'), # identifier assigned to an artist by the J. Paul Getty Museum
  'P2582' => wdpf ('P2582', 'J. Paul Getty Museum object id'),

  # MusicBrainz
  'P434'  => wdpf ('P434', 'MusicBrainz artist id'),
  'P435'  => wdpf ('P435', 'MusicBrainz work id'),
  'P436'  => wdpf ('P436', 'MusicBrainz release group id'),
  'P1004' => wdpf ('P1004', 'MusicBrainz place id'),

  # misc.
  'P625'  => wdpf ('P625', 'Geo Coordinates'),

  # chemistry
  'P233' => wdpf ('P233', 'SMILES'), # Simplified Molecular Input Line Entry Specification
  'P234' => wdpf ('P234', 'InChI'),    # International Chemical Identifier
  'P235' => wdpf ('P235', 'InChIKey'), # A hashed version of the full standard InChI - designed to create an identifier that encodes structural information and a can also be practically used in web searching.
  'P2017'  => wdpf ('P2017', 'isomeric SMILES'),
  # note: there are also Canonical SMILES, but no property for that yet

  'P248'  => wdpf ('P248', 'stated in'),
  'P577'  => wdpf ('P577', 'publication date'),

  # classification
  'P225' => wdpf ('P225', 'taxon name'),

  # astronomy
  'P716' => wdpf ('P716' => 'JPL Small-Body Database identifier'),

  # Software
  'P1072' => wdpf ('P1072' => 'readable file format'),
  'P1073' => wdpf ('P1073' => 'writable file format'),
  'P1195' => wdpf ('P1195' => 'file extension'),
);
my @filters= sort keys %filters;

# Property Bitmap Table
my @id_prop= (); # bitmap table
my $max_id= -1;
my $max_prop= 2000;

if ($exp_bitmap)
{
  my $BM_file= '@id_prop.bitmap';
  print "saving bitmap [$BM_file]\n";
  open (BM_FILE, '>:raw', $BM_file) or die "can't write to [$BM_file]\n";
}

my $fo_rec= new FDS('out_pattern' => "$out_dir/wdq%05d");
my $fo_count= $fo_rec->open();
my $fo_pos= 0;

<FI>;
my $pos;
LINE: while (1)
{
  $pos= tell(FI);
  my $l= <FI>;
  last unless (defined ($l));

  if ($fo_pos >= $OUT_CHUNK_SIZE)
  {
    $fo_count= $fo_rec->open();
    $fo_pos= 0;
  }
  $fo_pos= $fo_rec->tell();

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
  my $id_num;

  if ($id =~ m#^P(\d+)$#)
  {
    $id_num= undef;
  }
  elsif ($id =~ m#^Q(\d+)$#)
  {
    $id_num= $1;
    $max_id= $id_num if ($id_num > $max_id);
  }
  else
  {
    print "WARNING: id=[$id]: format incorrect\n";
    next LINE;
  }

  $types{$ty}++;

  if ($ty eq 'property')
  {
    # $pos= tell(FI);
    push (@{$props{$id}}, $j);
    next LINE;
  }

  if ($ty ne 'item' || !defined ($id_num))
  {
    print "[$line] [$pos] unknown type=[$ty]\n";
    print DIAG "[$line] [$pos] type=[$ty] line=[$line]\n";
    # $pos= tell(FI);
    next LINE;
  }

  # my $py= substr($l, 0, 30) . '...' . substr ($l, -30);
  my $px= $fo_rec->print($l);
  my $fo_pos_end= $fo_rec->tell();
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

  # claims -> properties
  my @all_properties= sort keys %$jc;

  # properties filtered
  my @found_properties= ();
  my @bm_row=(); for (my $i= 0; $i <= $max_prop; $i++) { $bm_row[$i]='.' }

  # foreach my $property (@filters)
  PROP: foreach my $property (@all_properties)
  {
    my $prop_num;
    if ($property =~ m#^P(\d+)$#)
    {
      $prop_num= $1;
      $max_prop= $prop_num if ($prop_num > $max_prop);
    }
    else
    {
      print "WARNING: property=[$property]: format incorrect\n";
      next PROP;
    }
    $id_prop[$id_num]->[$prop_num]++ if ($exp_bitmap == 1);
    $bm_row[$prop_num]='#' if ($exp_bitmap == 2);

    # if (exists ($jc->{$property}))
    if (exists ($filters{$property}))
    {
      my $fp= $filters{$property};
      # print "fp: ", Dumper ($fp);
      my $p= $jc->{$property};
      # print "p: ", Dumper ($p);

      my $x;
      eval { $x= $p->[0]->{'mainsnak'}->{'datavalue'}->{'value'} };

      # print "x: ", Dumper ($x); # exit;

      if ($@)
      {
        print DIAG "id=$id error: property=[$property] $x=[$x] e=[$@] property=", Dumper ($p);
      }
      elsif (!defined ($x))
      {
        print DIAG "id=$id undef x: property=[$property] property=", Dumper ($p);
      }
      else
      {
  # ZZZ
        push (@found_properties, $property);

        my $y= $fp->extract($x);

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
                 $lang_l, $pref_l,
                 join (',', @found_properties),
                 join (',', @all_properties),
                 ),
                 "\n";

  printf BM_FILE ("%09d\t", $id_num);
  print BM_FILE join ('', @bm_row);
  print BM_FILE "\n";

  last if (defined ($MAX_INPUT_LINES) && $line >= $MAX_INPUT_LINES); ### DEBUG
  # $pos= tell(FI);
}

close (FI);
$fo_rec->close();

# check if there are multiple definitions of the same property and flatten the structure a bit
open (PROPS_LIST, '>:utf8', $data_dir . '/props.csv') or die;
print PROPS_LIST join ($TSV_SEP, qw(prop def_cnt use_cnt datatype label_en descr_en)), "\n";

my @prop_ids= sort { $a <=> $b } map { ($_ =~ m#^P(\d+)$#) ? $1 : undef } keys %props;

foreach my $prop_num (@prop_ids)
# foreach my $prop_num (sort keys %props)
{
  my $prop_id= 'P'.$prop_num;
  my @prop= @{$props{$prop_id}};
  my $p0= $prop[0];
  if (@prop != 1) # each property needs to be defined exactly once
  {
    print "ATTN: prop=[$prop_num] count=",(scalar @prop), "\n";
  }
  else
  {
    $props{$prop_num}= $p0;
  }

  my $dt= $p0->{'datatype'};
  my $l_en= $p0->{'labels'}->{'en'}->{'value'};
  my $d_en= $p0->{'descriptions'}->{'en'}->{'value'};
  print PROPS_LIST join ($TSV_SEP, $prop_id, (scalar @prop), $prop_claims{$prop_id}, $dt, $l_en, $d_en), "\n";
}
close (PROPS_LIST);

open (PROPS, '>:utf8', $data_dir . '/props.json') or die;
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

print "max_id: $max_id\n";
print "max_prop: $max_prop\n";
print "lines: $line\n";
print "fo_count: $fo_count\n";

  if ($exp_bitmap == 1)
  {
   # ID to property mapping bitmap
   for (my $id= 1; $id <= $max_id; $id++)
   {
     my $row= $id_prop[$id];
     printf BM_FILE ("%09d\t", $id);
     for (my $prop= 0; $prop <= $max_prop; $prop++)
     # foreach my $prop (@prop_ids)
     {
       my $val= $row->[$prop];
       if ($val < 0 || $val > 1)
       {
         print "warning: invalid count id=[$id] prop=[$prop]\n";
       }
       print BM_FILE ($val) ? '#' : '.';
     }
     print BM_FILE "\n";
   }
   # print "prop_ids: ", join (' ', @prop_ids), "\n";
  }

  close (BM_FILE) if ($exp_bitmap);

  my $t_end= time();
  print "started:  ", scalar localtime ($t_start), "\n";
  print "finished: ", scalar localtime ($t_end), "\n";
  print "duration: ", $t_end-$t_start, " seconds\n";
}

sub counter
{
  my $s= shift;
  my $a= shift;

  my @s= keys %$s; my $c_s= @s;
  foreach my $x (@s) { $a->{$x}++; }
  $c_s;
}

