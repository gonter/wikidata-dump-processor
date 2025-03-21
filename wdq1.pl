#!/usr/bin/perl

use strict;

use JSON;

use Data::Dumper;
$Data::Dumper::Indent= 1;
use FileHandle;

binmode( STDOUT, ':utf8' ); autoflush STDOUT 1;
binmode( STDERR, ':utf8' ); autoflush STDERR 1;
binmode( STDIN,  ':utf8' );

use lib 'lib';
use WikiData::Utils;
use WikiData::Property::Filter;

use FDS;

my $TSV_SEP= "\t";
# my $OUT_CHUNK_SIZE= 500_000_000; # size of files containing item data in JSON format
# my $OUT_CHUNK_SIZE= 640_000_000; # size of files containing item data in JSON format
my $OUT_CHUNK_SIZE= 2_100_000_000; # size of files containing item data in JSON format
my $MAX_INPUT_LINES= undef;
# my $MAX_INPUT_LINES= 1_000_000; # for debugging to limit processing time; TODO: add commandline option; takes about 6 mintues for 100000 items

my $exp_bitmap= 0; # 1..does not work; 2..makes no sense, too sparsely populated arrays
# not used my $LR_max_propid= 1930; # dump from 20150608

my $seq= 'a';
my $date= '2021-04-28'; # maybe a config file should be used to set up the defaults...
my $content= 'data'; # or 'lexemes'

my ($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq);
my $upd_paths= 0;

my @langs= qw(en de ja it fr nl es hu pl);

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
    elsif ($an eq 'content')  { $content=  $av || shift (@ARGV); }
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

  # statistics
  my %types;
  my %attrs;
  my %count_snaktype;

  # item statistics
  my %lang_labels;
  my %lang_descr;
  my %lang_aliases;
  my %prop_claims;
  my %name_sitelinks;
  my %lang_lemmas;

  my %props;

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

  my $diag_file= $data_dir.'/@diag';
  open (DIAG, '>:utf8', $diag_file) or die "can't write diag file=[$diag_file]";

  my @item_attrs= qw(labels descriptions aliases claims sitelinks lemmas);

  my $running= 1;
  $SIG{INT}= sub { $running= 0; };

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

  # item list
  my $fnm_items= $data_dir . '/items_unsorted.tsv';

  local *FO_ITEMS;
  open (FO_ITEMS, '>:utf8', $fnm_items) or die "can't write to [$fnm_items]";
  my @cols1= qw(line pos fo_count fo_pos_beg fo_pos_end id type revid cnt_label cnt_desc cnt_aliases cnt_claims cnt_sitelink cnt_lemmas lang label);
  print FO_ITEMS join ($TSV_SEP, @cols1, qw(filtered_props claims)), "\n";
  # autoflush FO_ITEMS 1;

  local *FO_LABELS;
  my $fnm_labels= $data_dir . '/labels_unsorted.tsv';
  open (FO_LABELS, '>:utf8', $fnm_labels) or die "can't write to [$fnm_labels]";
  print FO_LABELS join ($TSV_SEP, qw(id P31 authctrl), @langs), "\n";

  # properties
  my @cols_filt= (@cols1, 'val');

  sub wdpf
  {
    my $prop= shift;
    my $label= shift;
    my $transform= shift;

    my $fnm_prop= $data_dir . '/' . $prop . '.tsv';

    return new WikiData::Property::Filter ('property' => $prop, 'label' => $label , 'cols' => \@cols_filt, 'transform' => $transform, 'filename' => $fnm_prop);
  }

  my %filters= ();

=begin comment

no filters; test 2020-10-11

  %filters=
  (
    # structure
    'P31'   => wdpf ('P31', 'instance of', 1),
    'P279'  => wdpf ('P279', 'subclass of', 1),
    'P360'  => wdpf ('P360', 'is a list of', 1),
    'P361'  => wdpf ('P361', 'part of', 1),
    'P1269' => wdpf ('P1269', 'facet of', 1),
    'P2429' => wdpf ('P2429', 'expected completeness', 1), # wikibase-item describes whether a property is intended to represent a complete set of real-world items having that property

    # item identifer (persons, places, etc.)
    'P213'  => wdpf ('P213', 'ISNI'), # International Standard Name Identifier for an identity
    'P227'  => wdpf ('P227', 'GND identifier'),
    'P244'  => wdpf ('P244', 'LCAuth ID'),  # Library of Congress ID for authority control (for books use P1144) # aka LCNAF, person, places, institutions, etc.
    'P1144' => wdpf ('P1144', 'LCCN'),      # Library of Congress Control Number (LCCN) (bibliographic) # publications only?
    'P1245' => wdpf ('P1245', 'OmegaWiki Defined Meaning'), # "Defined Meaning" on the site OmegaWiki
    'P3266' => wdpf ('P3266', 'LocFDD ID'), # Library of Congress Format Description Document ID # sounds interesting

    # person identifiers
    'P214'  => wdpf ('P214', 'VIAF identifier'),
    'P496'  => wdpf ('P496', 'ORCID identifier'),
    'P651'  => wdpf ('P651', 'Biografisch Portaal number'), # identifier at Biografisch Portaal van Nederland
    'P2280' => wdpf ('P2280', 'Austrian Parliament ID'), # identifier for an individual, in the Austrian Parliament's "Who's Who" database
    'P3421' => wdpf ('P3421', 'Belvedere artist ID'), # identifier assigned to an artist by the Österreichische Galerie Belvedere in Vienna

    # personal data?
    'P569'  => wdpf ('P569', 'Date of birth'),
    'P570'  => wdpf ('P570', 'Date of death'),
    'P2298' => wdpf ('P2298', 'NSDAP membership number (1925-1945)'),
    'P53'   => wdpf ('P53', 'family'), # not the same as P734 family name
    'P734'  => wdpf ('P734', 'family name'),
    'P735'  => wdpf ('P735', 'given name'),

    # arXiv.org
    'P818'  => wdpf ('P818', 'arXiv ID'),
    'P820'  => wdpf ('P820', 'arXiv classification'),

    # permanent identifiers
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
    'P966'  => wdpf ('P966', 'MusicBrainz label ID'),
    'P982'  => wdpf ('P982', 'MusicBrainz area ID'),
    'P1004' => wdpf ('P1004', 'MusicBrainz place id'),
    'P1407' => wdpf ('P1407', 'MusicBrainz series id'),
    'P4404' => wdpf ('P4404', 'MusicBrainz recording id'),
    'P5813' => wdpf ('P5813', 'MusicBrainz release id'),

    # AllMusic
    'P1728' => wdpf ('P1728', 'AllMusic artist ID'),
    'P1729' => wdpf ('P1728', 'AllMusic album ID'),
    'P1730' => wdpf ('P1730', 'AllMusic song ID'),
    'P1994' => wdpf ('P1994', 'AllMusic composition ID'),
    'P6110' => wdpf ('P6110', 'AllMusic release ID'),
    'P6306' => wdpf ('P6306', 'AllMusic performance ID'),
    
    # Google Play Music
    'P4198' => wdpf ('P4198', 'Google Play Music artist ID'),
    'P4199' => wdpf ('P4199', 'Google Play Music album ID'),

    # Amazon Music database
    'P6276' => wdpf ('P6276', 'Amazon Music artist ID'),

    # Books
    'P2607' => wdpf ('P2607', 'BookBrainz creator ID'), # identifier for a creator per the BookBrainz open book encyclopedia
    'P123'  => wdpf ('P123', 'publisher'), # organization or person responsible for publishing books, periodicals, games or software

    # WorldCat
    'P2163' => wdpf ('P163', 'FAST-ID'), # authority control identifier in WorldCat's “FAST Linked Data” authority file

    # Geography
    'P625'  => wdpf ('P625',  'Geo Coordinates'),
    'P1566' => wdpf ('P1566', 'GeoNames ID'),
    'P964'  => wdpf ('P964',  'Austrian municipality key'), # identifier for municipalities in Austria
    'P1282' => wdpf ('P1282', 'OSM tag or key'),

    # chemistry
    'P233' => wdpf ('P233', 'SMILES'),   # Simplified Molecular Input Line Entry Specification
    'P234' => wdpf ('P234', 'InChI'),    # International Chemical Identifier
    'P235' => wdpf ('P235', 'InChIKey'), # A hashed version of the full standard InChI - designed to create an identifier that encodes structural information and a can also be practically used in web searching.
    'P2017'  => wdpf ('P2017', 'isomeric SMILES'),
    # note: there are also Canonical SMILES, but no property for that yet

    'P248'  => wdpf ('P248', 'stated in'),
    'P577'  => wdpf ('P577', 'publication date'),

    # classification, taxonomy
    'P225'  => wdpf ('P225',  'taxon name'),
    'P1420' => wdpf ('P1420', 'taxon synonym'),
    'P1843' => wdpf ('P1843', 'taxon common name'),

    # astronomy
    'P716' => wdpf ('P716' => 'JPL Small-Body Database identifier'),
    'P744' => wdpf ('P744' => 'asteroid family'),
    'P881' => wdpf ('P881' => 'type of variable star'),
    'P999' => wdpf ('P999' => 'ARICNS'), # identifier for stars in ARICNS
    'P5736' => wdpf ('P5736' => 'Minor Planet Center body ID'),

  # 'P2211' => wdpf ('P2211' => 'position angle'), # xx
  # 'P2212' => wdpf ('P2212' => 'angular distance'), # xx
  # 'P2213' => wdpf ('P2213' => 'longitude of ascending node'), # xx
  # 'P2215' => wdpf ('P2215' => 'proper motion'), # xx
  # 'P2216' => wdpf ('P2216' => 'radial velocity'), # xx
  # 'P4296' => wdpf ('P4296' => 'stellar rotational velocity'), # xx

    # Software
    'P1072' => wdpf ('P1072' => 'readable file format'),
    'P1073' => wdpf ('P1073' => 'writable file format'),
    'P1195' => wdpf ('P1195' => 'file extension'),

    # external-id
    'P503' => wdpf ('P503' => 'ISO standard'), # number of the ISO standard which normalizes the object

    # URLs
    'P854' => wdpf ('P854' => 'reference URL'),
    'P856' => wdpf ('P856' => 'official website'),
    'P953' => wdpf ('P953' => 'full text available at'),
    'P973' => wdpf ('P973' => 'described at URL'),
    'P1019' => wdpf ('P1019' => 'feed URL'),
    'P1065' => wdpf ('P1065' => 'archive URL'),
    'P1324' => wdpf ('P1324' => 'source code repository'),
    'P1325' => wdpf ('P1325' => 'external data available at'),
    'P1401' => wdpf ('P1401' => 'bug tracking system'),
    'P1581' => wdpf ('P1581' => 'official blog'),
    'P2699' => wdpf ('P2699' => 'URL'),

  );

=end comment
=cut

  if ($content eq 'data')
  {
    %filters=
    (
      # structure
      'P31'   => wdpf ('P31', 'instance of', 1),
      'P279'  => wdpf ('P279', 'subclass of', 1),
      'P360'  => wdpf ('P360', 'is a list of', 1),
      'P361'  => wdpf ('P361', 'part of', 1),
      'P1269' => wdpf ('P1269', 'facet of', 1),

      # item identifer (persons, places, etc.)
      'P213'  => wdpf ('P213', 'ISNI'), # International Standard Name Identifier for an identity
      'P227'  => wdpf ('P227', 'GND identifier'),
      'P243'  => wdpf ('P243', 'OCLC control number'),  # identifier for a unique bibliographic record in OCLC WorldCat
      'P244'  => wdpf ('P244', 'LCAuth ID'),  # Library of Congress ID for authority control (for books use P1144) # aka LCNAF, person, places, institutions, etc.
      'P2833' => wdpf ('P2833', 'ARKive ID'), # identifier for a taxon, in the ARKive database
      'P8080' => wdpf ('P8080', 'Ökumenisches Heiligenlexikon ID'), # identifier for a saint in the database Ökumenisches Heiligenlexikon

      # person identifiers
      'P214'  => wdpf ('P214', 'VIAF identifier'),
      'P496'  => wdpf ('P496', 'ORCID identifier'),

      # personal data?
      'P569'  => wdpf ('P569', 'Date of birth'),
      'P570'  => wdpf ('P570', 'Date of death'),

      # other
      'P6782'  => wdpf ('P6782', 'ROR ID'),
      'P5748'  => wdpf ('P5748', 'Basisklassifikation'), # corresponding class in the Basisklassifikation library classification

      # Geography
      'P625'  => wdpf ('P625',  'Geo Coordinates'),
      'P1566' => wdpf ('P1566', 'GeoNames ID'),
      'P964'  => wdpf ('P964',  'Austrian municipality key'), # identifier for municipalities in Austria
      'P1282' => wdpf ('P1282', 'OSM tag or key'),

      # publications
      'P356'  => wdpf ('P356', 'DOI'),
      'P4109' => wdqf ('P4109', 'URN:NBN'),

      # WoRMS database (2021-01-31)
      'P850'  => wdpf ('P850',  'WoRMS-ID for taxa'), # 2021-01-31: 442262 items
      'P3860' => wdpf ('P3860', 'Wormbase Gene ID'),  # 2021-01-31:  20449 items
      'P6678' => wdpf ('P6678', 'WoRMS source ID '),  # 2021-01-31:    639 items

      # URLs
      'P854' => wdpf ('P854' => 'reference URL'),
      'Punivie' => wdpf ('Punivie' => 'mention of univie.ac.at', 1),

      # publications
      'P212'  => wdpf ('P212', 'ISBN-13'),
      'P236'  => wdpf ('P212', 'ISSN'),
      'P345'  => wdpf ('P345', 'IMDb identifier'),
      'P356'  => wdpf ('P356', 'DOI'),
      'P698'  => wdpf ('P698', 'PubMed ID'), # identifier for journal articles/abstracts in PubMed
      'P957'  => wdpf ('P957', 'ISBN-10'),
      'P3035' => wdpf ('P3035', 'ISBN publisher prefix'), # ISBN publisher prefix
      'P3097' => wdpf ('P3097', 'ISBN identifier group'), # ISBN prefix for countries or languages
      'P3212' => wdpf ('P3212', 'ISAN'), # unique identifier for audiovisual works and related versions, similar to ISBN for books

      # Extras, see discussion
      'P935' => wdpf ('P935' => 'Commons gallery'),
      'P373' => wdpf ('P373' => 'Commons categroy'),

    );
  }
  elsif ($content eq 'lexemes')
  {
    %filters=
    (
      'P31'   => wdpf ('P31', 'instance of', 1),
      'Punivie' => wdpf ('Punivie' => 'mention of univie.ac.at', 1),
    );
  }

  # my %filters= ();
  my @filters= sort keys %filters;

=begin comment

meta-properties: properties about properties

  'P2429' => wdpf ('P2429', 'expected completeness', 1), # describes whether a property is intended to represent a complete set of real-world items having that property
  this points to several values, e.g.:
  Q21873886 => will always be incomplete
  Q21873974 => will eventually be incomplete

=end comment
=cut

  # Authority Control
  my @authctrl= qw(P213 P214 P227 P244 P496 P6782);
  # my %authctrl= map { $_ => 1 } @authctrl;
  my %authctrl_props= map { $_ => 1 } (@authctrl, qw(P19 P20 P21 P31 P569 P570));

  my $fnm_authctrl= $data_dir . '/authctrl.json';

  local *FO_AUTHCTRL;
  open (FO_AUTHCTRL, '>:utf8', $fnm_authctrl) or die "can't write to [$fnm_authctrl]";
  # autoflush FO_AUTHCTRL 1;
  print FO_AUTHCTRL "[\n";
  my $cnt_authctrl= 0;

  # properties

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

  # local *FO_rec_debug; open (FO_rec_debug, '>:raw', join('/', $out_dir, '@fo_rec_debug'));
  <FI>;
  my $pos;
  LINE: while ($running)
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
    printf ("%s %9ld %12ld %3d %12ld\n", scalar localtime(time()), $line, $pos, $fo_count, $fo_pos) if (($line % 10_000) == 0);

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

    my ($id, $ty, $revid)= map { $j->{$_} } qw(id type lastrevid);

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
    elsif ($id =~ m#^L(\d+)$#)
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
      print __LINE__, " prop id=[$id]\n";
      push (@{$props{$id}}, $j);
      next LINE;
    }

    if (($ty ne 'item' && $ty ne 'lexeme') || !defined ($id_num))
    {
      print "[$line] [$pos] unknown type=[$ty]\n";
      print DIAG "[$line] [$pos] type=[$ty] line=[$line]\n";
      # $pos= tell(FI);
      next LINE;
    }

    # my $py= substr($l, 0, 30) . '...' . substr ($l, -30);
    # print FO_rec_debug $l, "\n"; # this should be one json record per line
    my $px= $fo_rec->print($l);
    my $fo_pos_end= $fo_rec->tell();
    # print "px=[$px] l=[$py]\n";

    foreach my $a (keys %$j) { $attrs{$a}++; }

    # grip and counts labels and descriptions
    my ($jl, $jd, $ja, $jc, $js, $jle)= map { $j->{$_} } @item_attrs;

    my $c_jl= counter ($jl, \%lang_labels);
    my $c_jd= counter ($jd, \%lang_descr);
    my $c_ja= counter ($ja, \%lang_aliases);
    my $c_jc= counter ($jc, \%prop_claims);
    my $c_js= counter ($js, \%name_sitelinks);
    my $c_jle= counter ($jle, \%lang_lemmas);

    # language translations
    my (%tlt_l, %tlt_d);
    my ($pref_l, $lang_l);

    my @x_langs= @langs;
    if (defined ($jle))
    {
      my @x= keys %$jle;
      # print __LINE__, " x: ", join(', ', @x), "\n";
      push (@x_langs, @x);
    }

    foreach my $lang (@x_langs)
    {
      my $label= (defined ($jle)) ? $jle->{$lang}->{value} : $jl->{$lang}->{value};
      my $desc=  $jd->{$lang}->{value};
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

    # Authority Control
    my $P31val;
    my $authctrl;
    if ($ty eq 'item')
    {
      my $use_authctrl= 0;
      foreach my $x (@authctrl)
      {
        if (exists ($jc->{$x}))
        {
          $use_authctrl= 1;
          last;
        }
      }

      # if (!$use_authctrl && exists ($jc->{P31}))
      if (exists ($jc->{P31}))
      {
        my $P31= $jc->{P31};
        $P31val= $P31->[0]->{mainsnak}->{datavalue}->{value}->{id};
        # print __LINE__, " P31=[$P31] => [$P31val]\n";
        $use_authctrl= 1 if ($P31val eq 'Q5');
      }

      if ($use_authctrl)
      {
          $authctrl=
          {
            'id' => $id,
            'tlt_l' => \%tlt_l,
            'tlt_d' => \%tlt_d,
            # P31 => $P31,
          };
      }
    }

    # experimental filter
    my $exp_filter= 0;
    if ($l =~ m#"([^"]*univie\.ac\.at[^"]*)"#i)
    {
      my $y= $1;
      $y=~ s/\\//g; # this is a JSON fragment which encodes / as \/
      print __LINE__, " y=[$y]\n";
      my $fp= $filters{Punivie};
      push (@found_properties, 'Punivie');

      local *FO_p= $fp->{'_FO'};
      print FO_p join ($TSV_SEP,
                 $line, $pos, $fo_count, $fo_pos, $fo_pos_end,
                 $id, $ty, $revid,
                 $c_jl, $c_jd, $c_ja, $c_jc, $c_js, $c_jle,     # counters
                 $lang_l, $pref_l,
                 $y,
                 ), "\n";
    }

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

        my $p= $jc->{$property};
        # print "p: ", Dumper ($p);

        my $ms;
        eval { $ms= $p->[0]->{mainsnak} };
        if ($@)
        {
          print DIAG "id=$id ERROR: no mainsnak element; property=[$property] e=[$@] property=", Dumper ($p);
          next PROP;
        }

        my $snaktype= $ms->{snaktype};
        $count_snaktype{$snaktype}++;
        if ($snaktype ne 'value')
        {
          print DIAG "id=$id NOTE: snaktype=[$snaktype], property=[$property]\n";
          next PROP;
        }

        my $x;
        eval { $x= $ms->{datavalue}->{value} };

        # print "x: ", Dumper ($x); # exit;

        if ($@)
        {
          print DIAG "id=$id ERROR: no value element; property=[$property] $x=[$x] e=[$@] property=", Dumper ($p);
          next PROP;
        }
        elsif (!defined ($x))
        {
          print DIAG "id=$id NOTE: undef property value: property=[$property] property=", Dumper ($p);
          next PROP;
        }

      my $y;
      if (exists ($filters{$property}))
      {
        my $fp= $filters{$property};
        # print "fp: ", Dumper ($fp);

    # ZZZ
          push (@found_properties, $property);

          $y= $fp->extract($x);

          local *FO_p= $fp->{'_FO'};
          print FO_p join ($TSV_SEP,
                 $line, $pos, $fo_count, $fo_pos, $fo_pos_end,
                 $id, $ty, $revid,
                 $c_jl, $c_jd, $c_ja, $c_jc, $c_js, $c_jle,     # counters
                 $lang_l, $pref_l,
                 $y,
                 ), "\n";
      }
      else
      { # BUG HERE: this returns strange data; TODO: debugging!
        $y= WikiData::Property::Filter::_extract ($x, (ref($x) eq 'HASH') ? 1 : 0);
      }

      if (defined ($authctrl))
      {
        # collect all filtered properties for the authority record
        # $authctrl->{$property}= $y;

        # only collect properties which are actually used as authority record
        $authctrl->{$property}= $y if (exists($authctrl_props{$property}));
      }
    }

    # TODO: count claims, aliases, sitelinks, etc.

    # print "[$line] [$pos] ", Dumper ($j) if ($ty eq 'property');
    print FO_ITEMS join ($TSV_SEP,
                   $line, $pos, $fo_count, $fo_pos, $fo_pos_end,
                   $id, $ty, $revid,
                   $c_jl, $c_jd, $c_ja, $c_jc, $c_js, $c_jle,     # counters
                   $lang_l, $pref_l,
                   join (',', @found_properties),
                   join (',', @all_properties),
                   ),
                   "\n";

    printf BM_FILE ("%09d\t", $id_num);
    print BM_FILE join ('', @bm_row);
    print BM_FILE "\n";

    if (defined ($authctrl))
    {
      print FO_AUTHCTRL ",\n" if ($cnt_authctrl);
      # print FO_AUTHCTRL encode_json($authctrl);
      my $json= new JSON;
      print FO_AUTHCTRL $json->allow_blessed->convert_blessed->encode($authctrl);

      $cnt_authctrl++;
      printf ("%s %9ld authority control records\n", scalar localtime(time()), $cnt_authctrl)  if (($cnt_authctrl % 1000) == 0);
    }

    # export labels
    {
      my @labels_langs= keys %tlt_l;
      if (@labels_langs)
      {
        print FO_LABELS join ($TSV_SEP,
                              $id, $P31val,
                              (defined($authctrl)) ? join(',', sort keys %$authctrl) : '',
                              (map { $tlt_l{$_} } @langs)), "\n";
      }
    }

    $running= 0 if (defined ($MAX_INPUT_LINES) && $line >= $MAX_INPUT_LINES); ### DEBUG
    # $pos= tell(FI);
  }

  close (FI);
  $fo_rec->close();

  print "$cnt_authctrl authority records written to $fnm_authctrl\n";
  print FO_AUTHCTRL "\n]\n";
  close (FO_AUTHCTRL);

  # check if there are multiple definitions of the same property and flatten the structure a bit
  open (PROPS_LIST, '>:utf8', $data_dir . '/props.tsv') or die;
  print PROPS_LIST join ($TSV_SEP, qw(prop def_cnt use_cnt datatype label_en descr_en)), "\n";

  # print __LINE__, " props: ", Dumper(\%props);
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

    my $dt= $p0->{datatype};
    my $l_en= $p0->{labels}->{en}->{value};
    my $d_en= $p0->{descriptions}->{en}->{value};
    print PROPS_LIST join ($TSV_SEP, $prop_id, (scalar @prop), $prop_claims{$prop_id}, $dt, $l_en, $d_en), "\n";
  }
  close (PROPS_LIST);

  if (open (PROPS, '>:utf8', $data_dir . '/props.json'))
  {
    # print PROPS encode_json (\%props);
    my $json= new JSON;
    print PROPS $json->allow_blessed->convert_blessed->encode(\%props);
    close (PROPS);
  }

  my $stats_opened= 0;
  if (open (STATS, '>:utf8', $data_dir . '/conversion-stats.log'))
  {
    $stats_opened= 1;
    print STATS "pos: $pos\n";
    print STATS "types: ", Dumper (\%types);
    print STATS "attrs: ", Dumper (\%attrs);
    print STATS "lang_labels: ", Dumper (\%lang_labels);
    print STATS "lang_descr: ", Dumper (\%lang_descr);
    print STATS "lang_aliases: ", Dumper (\%lang_aliases);
    print STATS "lang_lemmas: ", Dumper (\%lang_lemmas);
    print STATS "name_sitelinks: ", Dumper (\%name_sitelinks);
    print STATS "prop_claims: ", Dumper (\%prop_claims);

    print STATS "max_id: $max_id\n";
    print STATS "max_prop: $max_prop\n";
    print STATS "lines: $line\n";
    print STATS "fo_count: $fo_count\n";
    print STATS "cnt_authctrl: $cnt_authctrl\n";
    print STATS "snaktypes: ", Dumper (\%count_snaktype);
  }

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

  if ($stats_opened)
  {
    print STATS "started:  ", scalar localtime ($t_start), "\n";
    print STATS "finished: ", scalar localtime ($t_end), "\n";
    print STATS "duration: ", $t_end-$t_start, " seconds\n";
    close (STATS);
  }
}

sub counter
{
  my $s= shift;
  my $a= shift;

  my @s= keys %$s; my $c_s= @s;
  foreach my $x (@s) { $a->{$x}++; }
  $c_s;
}

