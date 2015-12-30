#!/usr/bin/perl

use strict;

use lib 'lib';

use JSON;
use Compress::Zlib;

use Util::Simple_CSV;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use WikiData::Utils;

my $seq= 'a';
my $date= '2015-12-28';
my ($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq);

my $upd_paths= 0;

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

usage() unless (@PARS);

($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq) if ($upd_paths);
my $fnm_items= join ('/', $data_dir, 'items.csv');

my %IDS= map { $_ => 1 } @PARS;

print "IDS: ", Dumper (\%IDS);
my $csv= new Util::Simple_CSV (separator => "\t");

local *FI;
my $fi_open;
(*FI, $fi_open)= $csv->open_csv_file ($fnm_items);
print "fi_open=[$fi_open]\n";
$csv->load_csv_file_headings (*FI);

my $idx_id= $csv->{'index'}->{'id'};

sub filter
{
  my $row= shift;
  
  return (exists ($IDS{$row->[$idx_id]})) ? 1 : 0;
}

$csv->set ( filter => \&filter, max_items => scalar @PARS);
$csv->load_csv_file_body (*FI);
close (FI);

print "csv: ", Dumper ($csv);

# TODO: order data by fo_count and fo_pos_beg!

foreach my $row (@{$csv->{'data'}})
{
  load_item ($row);
}

exit(0);

sub usage
{
  system ('perldoc', $0);
  exit;
}

sub load_item
{
  my $row= shift;

  print "row: ", Dumper ($row);
  
  my ($id, $num, $beg, $end)= map { $row->{$_} } qw(id fo_count fo_pos_beg fo_pos_end);
  my $size= $end-$beg;
  my $fnm_data= sprintf ('%s/wdq%05d.cmp', $out_dir, $row->{'fo_count'});

  print "id=[$id] num=[$num] fnm_data=[$fnm_data] beg=[$beg] end=[$end] size=[$size]\n";

  open (FD, '<:raw', $fnm_data);
  seek (FD, $beg, 0);
  my $buffer;
  sysread (FD, $buffer, $size);
  my $json= uncompress ($buffer);
  # print "json: ", Dumper ($json);
  my $data= JSON::decode_json ($json);
  print "data: ", Dumper ($data);

  $data;
}

