#!/usr/bin/perl

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

use WikiData::Utils;
use Wiktionary::Utils;
use PDS;

my $date; # = '2024-01-01';
my $seq= 'a';
my $lang= undef;
my $cmp_fnm_pattern= '%s/wdq%05d.cmp';

my $find_column= 'label';
# my $op_mode= 'find_items';
my $op_mode= 'get_items';
my @show_langs= qw(de de-at en it nl fr pt uk el ); # (ru ja zh ko)
my $tsv_out;
# my @filter_props= ();
my $filter_prop= ();
my $filter_mv_sep= ' ';

my $upd_paths= 1;

my $show_dumps= 0;
my $show_mode= 'json';
# my $show_mode= 'labels';
# my $DEBUG= 0;

# experimental: transcribe data
my $t_mode;
my $t_file;
local *T_FILE;

autoflush STDOUT 1;

my $export_file;
local *EXP;

my @PARS= ();
while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PARS, @ARGV); @ARGV=(); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($an, $av)= split ('=', $1, 2);
    # print "an=[$an] av=[$av]\n";

       if ($an eq 'date') { $date= $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'seq')  { $seq=  $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'lang') { $lang= $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'find') { $op_mode= 'find_items'; $find_column= $av || 'label' }
    elsif ($an eq 'save') { $tsv_out= $av || shift (@ARGV); }
    elsif ($an eq 'scan') { $op_mode= 'scan'; }
    elsif ($an eq 'export') { $export_file= $av || shift (@ARGV); }
    elsif ($an eq 't1') { $t_mode= 't1'; $t_file= $av || shift(@ARGV); }
  # elsif ($an eq 'filter') { my $x= $av || shift(@ARGV); @filter_props= split(',', $x); }
    elsif ($an eq 'filter') { $filter_prop= $av || shift(@ARGV); unshift (@ARGV, '-S'); }
    elsif ($an eq 'silent') { $show_mode= 'silent'; unshift (@ARGV, '-S'); }
    else
    {
      usage();
    }
  }
  elsif ($arg =~ /^-(.+)/)
  {
    foreach my $flag (split('', $1))
    {
         if ($flag eq 'L') { $show_mode= 'labels'; $show_dumps= 0; PDS::show_dumps(0); }
      elsif ($flag eq 'J') { $show_mode= 'json';   $show_dumps= 0; PDS::show_dumps(0); }
      elsif ($flag eq 'S') { $show_mode= 'silent'; $show_dumps= 0; PDS::show_dumps(0); }
      elsif ($flag eq 'D') { $show_mode= 'labels'; $show_dumps= 1; PDS::show_dumps(1); }
      else { usage(); }
    }
  }
  else { push (@PARS, $arg); }
}

if (defined ($date))
{
  $seq= 'a' unless (defined ($seq));
}
else
{
  $date= (substr($PARS[0], 0, 1) eq 'L') ? 'lexemes' : 'latest';
}

# prepare items list
my ($fnm, $data_dir, $out_dir, $prop_dir);
my $fnm_items;
if ($upd_paths)
{
  if (defined ($lang))
  { # must be Wiktionary, if there is a language defined ...
    ($fnm, $data_dir, $out_dir)= Wiktionary::Utils::get_paths ($lang, $date, $seq);
    print "ATTN: wiktionary mode!\n";
    $fnm_items= join ('/', $data_dir, "items.tsv");
    $cmp_fnm_pattern= '%s/wkt%05d.cmp';
  }
  else
  {
    ($fnm, $data_dir, $out_dir, $prop_dir)= WikiData::Utils::get_paths ($date, $seq);
    $fnm_items= join ('/', $data_dir, 'items.tsv');
  }
}
# print __LINE__, " date=[$date] seq=[$seq] data_dir=[$data_dir]\n";
# TODO: fails if there is no data at the given date/seq

my $csv= new Util::Simple_CSV (separator => "\t");

local *FI_csv;
my $fi_open;
local *TSV_filtered;
if ($op_mode eq 'scan' || $op_mode eq 'find_items')
{
  (*FI_csv, $fi_open)= $csv->open_csv_file ($fnm_items);
  # print "fi_open=[$fi_open]\n";
  $csv->{'__FI'}= *FI_csv;
  $csv->load_csv_file_headings (*FI_csv);
}

  if (defined ($filter_prop))
  {
    my $fnm_tsv=     join('/', $prop_dir, $filter_prop . '.tsv');
       $export_file= join('/', $prop_dir, $filter_prop . '.ndjson');
    print STDERR __LINE__, " filter_prop=[$filter_prop] fnm_tsv=[$fnm_tsv]\n";
    open(TSV_filtered, '>:utf8', $fnm_tsv) or die;
    # print TSV_filtered join("\t", qw(id lang label lastrevid modified pageid values)), "\n";
    print TSV_filtered join("\t", 'id', @show_langs, qw(lastrevid modified pageid values)), "\n";

    unless (@PARS)
    {
      my $fnm_prop= join('/', $prop_dir, $filter_prop);
      open (P, '<:utf8', $fnm_prop) or die;
      print STDERR " fnm_prop=[$fnm_prop]\n";
      while (my $p= <P>)
      {
        chop($p);
        push (@PARS, $p);
      }
      close(P);
    }
  }

  # paged data store for record index
  my $fnm_rec_idx= join ('/', $data_dir, 'records.idx');
  my $rec_size= 32;
  my $pds= new PDS (rec_size => $rec_size, backing_file => $fnm_rec_idx);
  # print "pds: ", Dumper ($pds);

  if (defined ($t_mode) && defined ($t_file))
  {
    open (T_FILE, '>>:utf8', $t_file) or die;
    print STDERR __LINE__, " saving transcript into $t_file\n";
  }

if (defined ($export_file))
{
  open (EXP, '>:utf8', $export_file) or die;
  print STDERR __LINE__, " export_file=[$export_file]\n";
}

if ($op_mode eq 'find_items')
{
  usage() unless (@PARS);
  find_items($csv, \@PARS, $find_column);
}
elsif ($op_mode eq 'get_items')
{
  usage() unless (@PARS);
  get_items($csv, \@PARS);
}
elsif ($op_mode eq 'scan')
{
  scan_items($csv);
}
else { usage(); }

close (FI_csv) if ($fi_open);

if (defined ($tsv_out))
{
  $csv->save_csv_file (filename => $tsv_out, separator => "\t");
}

exit(0);

sub scan_items
{
  my $csv= shift;

  my $index= $csv->{'index'};
  # print "index: ", Dumper ($index);

  my ($idx_id, $idx_fo_num, $idx_pos_beg, $idx_pos_end)= map { $index->{$_} } qw(id fo_count fo_pos_beg fo_pos_end);
  # print "idx_id=[$idx_id] idx_fo_num=[$idx_fo_num] idx_pos_beg=[$idx_pos_beg] idx_pos_end=[$idx_pos_end]\n";

  my $columns= $csv->{'columns'};
  # print "columns: ", Dumper ($columns);

  parse_idx_file ($csv->{'__FI'}, $idx_id, $idx_fo_num, $idx_pos_beg, $idx_pos_end);
}

sub parse_idx_file
{
  local *F_in= shift;

  my $idx_id= shift;
  my $idx_fo_num= shift;
  my $idx_pos_beg= shift;
  my $idx_pos_end= shift;

# print "parse_idx_file\n";

  my $last_rec_num= 0; # there is no Q0

  my ($cnt_total, $cnt_invalid, $cnt_ordered)= (0, 0, 0);
  sub print_stats
  {
    print "statistics: total=[$cnt_total] invalid=[$cnt_invalid] ordered=[$cnt_ordered]\n";
  }

  # designed/optimized values
  # my $page_block_factor= 4; # this is guess work; try to avoid skipping around

  # my $page_size= 1024*$rec_size*$page_block_factor;
  # my $page_size= 4*1024*1024; # 4 MByte blocks, depending on fileystem layout
  # my $page_hdr_size= 1024;

  # statistics
  LINE: while (<F_in>)
  {
    chop;
    my @f= split ("\t");

    my $pos_idx= tell(F_in);
    $cnt_total++;

    my ($id, $f_num, $beg, $end)= map { $f[$_] } ($idx_id, $idx_fo_num, $idx_pos_beg, $idx_pos_end);

    my $rec_num;
    if ($id =~ m#^Q(\d+)$#)
    { # Wikidata
      $rec_num= $1;
    }
    elsif ($id =~ m#^L(\d+)$#)
    { # Wikidata Lexemes
      $rec_num= $1;
    }
    elsif ($id =~ m#^(\d+)$#)
    { # Wiktionary
      $rec_num= $1;
    }
    else
    {
      print "unknown id format [$id] at line=[$cnt_total] offset=[$pos_idx] ", Dumper(\@f);
      $cnt_invalid++;
      next LINE;
    }

    my $rec_s= pack ('LLLLLLLL', $rec_num, $pos_idx, $f_num, $beg, $end, 0, 0, 0);
    $pds->store ($rec_num, $rec_s);

    next LINE;

    print "id=[$id] rec_num=[$rec_num] pos_idx=[$pos_idx] f_num=[$f_num] beg=[$beg] end=[$end]\n" if ($show_dumps);
  }

  $pds->flush_page();
  $pds->print_page_info();

  print_stats();
}

=head2 find_items($csv)

search for specific items and display their JSON structure

=cut

sub find_items
{
  my $csv= shift;
  my $pars= shift;
  my $find_column= shift;

  unless (defined ($pars) && @$pars)
  {
    print "no items specified\n";
    return undef;
  }

  my $idx_id= $csv->{'index'}->{$find_column};

  # print "idx_id=[$idx_id]\n";
  my %IDS= map { $_ => 1 } @$pars;
  print "IDS: ", Dumper (\%IDS);

  sub filter
  {
    my $row= shift;
  
    return (exists ($IDS{$row->[$idx_id]})) ? 1 : 0;
  }

  print "beginning loading of TSV\n";
  local *FI_csv= $csv->{'__FI'};
  $csv->set ( filter => \&filter, max_items => scalar @$pars);
  $csv->load_csv_file_body (*FI_csv);
  close (FI_csv);

  print "csv: ", Dumper ($csv);

  # TODO: order data by fo_count and fo_pos_beg!

  my $cnt_items= 0;
  foreach my $row (@{$csv->{'data'}})
  {
    load_item ($row); # TODO: check for errors etc.
    $cnt_items++;
  }

  return $cnt_items;
}

sub get_items
{
  my $csv= shift;   # NOTE: the csv file is not used in this mode!
  my $pars= shift;

  unless (defined ($pars) && @$pars)
  {
    print "no items specified\n";
    return undef;
  }

  $pds->{do_read}= 1;

  my @rec_nums=();
  foreach my $item (@$pars)
  {
    if ($item =~ m#^[QL](\d+)$#)
    {
      push (@rec_nums, $1);
    }
    elsif ($item =~ m#^(\d+)$#)
    {
      push (@rec_nums, $1);
    }
  }
  # print __LINE__, " recs: ", join (' ', @rec_nums), "\n";

  my $cnt_items= 0;
  foreach my $rec_num (sort { $a <=> $b } @rec_nums)
  {
    # print "rec_num=[$rec_num]\n";
    my $data= $pds->retrieve ($rec_num);
    main::hexdump ($data) if ($show_dumps);
    my ($x_rec_num, $pos_idx, $f_num, $beg, $end, @x)= unpack ('LLLLLLLL', $data);

    # recreate most importent parts of one row from items.tsv 
    my $row=
    {
      id         => 'Q'.$x_rec_num,
      fo_count   => $f_num,
      fo_pos_beg => $beg,
      fo_pos_end => $end,
    };
    print "row: ", Dumper ($row) if ($show_dumps);

    if ($x_rec_num > 0)
    {
      load_item ($row); # TODO: check for errors etc.
      $cnt_items++;
    }
    else
    {
      print "item not found in index\n";
      print "rec_num=[$rec_num] x_rec_num=[$x_rec_num] pos_idx=[$pos_idx] f_num=[$f_num] beg=[$beg] end=[$end]\n";
    }

  }

  return $cnt_items;
}

sub usage
{
  system ('perldoc', $0);
  exit;
}

sub load_item
{
  my $row= shift;

  # print "row: ", Dumper ($row);
  
  my ($id, $f_num, $beg, $end)= map { $row->{$_} } qw(id fo_count fo_pos_beg fo_pos_end);
  my $size= $end-$beg;
  my $fnm_data= sprintf ($cmp_fnm_pattern, $out_dir, $row->{'fo_count'});

  # print "id=[$id] f_num=[$f_num] fnm_data=[$fnm_data] beg=[$beg] end=[$end] size=[$size]\n";

  open (FD, '<:raw', $fnm_data);
  seek (FD, $beg, 0);
  my $buffer;
  sysread (FD, $buffer, $size);
  my $block= uncompress ($buffer);
  # print __LINE__, " block: ", Dumper ($block);
  utf8::decode($block);
  # print __LINE__, " block: ", Dumper ($block);
  if (defined ($export_file))
  {
    print EXP $block, "\n";
  }

  if (defined ($t_file))
  {
    print T_FILE $block, "\n";
  }

  if (defined ($lang))
  {
    # print "buffer: ", Dumper ($buffer);
    # print "block: ", Dumper (\$block);
    print '='x72, "\n", "block:\n", $block, "\n", '='x72, "\n";

    return $block;
  }
  else
  {
    my $data= JSON::decode_json ($block);
    # print __LINE__, " data: ", Dumper ($data);

    if (defined ($filter_prop))
    {
      my ($id, $claims, $labels, $lastrevid, $modified, $pageid)= map { $data->{$_} } qw(title claims labels lastrevid modified pageid);
      my $filter_claims= $claims->{$filter_prop};
      if (defined ($filter_claims))
      {
        my @values= get_snak_values($filter_claims);

=begin comment
        my ($label, $lang);
        my @langs= (qw(en de fr it mul zh), sort keys %$labels);
        while ($lang= shift (@langs))
        {
          if (exists($labels->{$lang})) { $label= $labels->{$lang}->{value}; last; }
        }

        print TSV_filtered join("\t", $id, $lang, $label, $lastrevid, $modified, $pageid, join($filter_mv_sep, @values)), "\n";
        # print STDERR join("\t", $id, $label, join($filter_mv_sep, @values)), "\n";

=end comment
=cut

        print TSV_filtered join("\t", $id, ( map { $labels->{$_}->{value} } @show_langs ), $lastrevid, $modified, $pageid, join($filter_mv_sep, @values)), "\n";

      }
    }

    if ($show_mode eq 'json')
    {
      print $block; # original is JSON!
    }
    elsif ($show_mode eq 'labels')
    {
      my ($l, $d, $a)= map { $data->{$_} } qw(labels descriptions aliases);
      foreach my $lang (@show_langs)
      {
        my $ll= get_value($l, $lang, 'value');
        my $dl= get_value($d, $lang, 'value');
        if (defined ($ll) || defined ($dl))
        {
          print join("\t", $lang, $ll, $dl), "\n";
        }
      }
    }
    elsif ($show_mode eq 'silent') {} # be silent
    else { print "unknown show_mode=[$show_mode]\n"; }

    return $data;
  }
}

sub get_value
{
  my $what= shift;
  my @selectors= @_;

  foreach my $s (@selectors)
  {
    my $x= $what->{$s};
    return undef unless (defined ($x));
    $what= $x;
  }
  $what;
}

sub get_snak_values
{
  my $filter_claims= shift;

  my @values;
  foreach my $fc (@$filter_claims)
  {
    my $v;
    my $ms= $fc->{mainsnak};
    my $dv= $ms->{datavalue};
    if ($ms->{snaktype} eq 'value')
    {
      if ($ms->{datatype} eq 'wikibase-item')
      {
        $v= $dv->{value}->{id};
      }
      else
      {
        $v= $dv->{value};
      }
    }
    else
    {
      $v= $ms->{snaktype}
    }

    push (@values, $v);
  }

  (wantarray) ? @values : \@values;
}
