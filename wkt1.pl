#!/usr/bin/perl

use strict;

use JSON;
use FileHandle;

use Util::JSON;
use Util::Simple_CSV;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use lib 'lib';
use wkutils;
use Wiktionary::Utils;

use FDS;

my $TSV_SEP= "\t";
# my $OUT_CHUNK_SIZE= 500_000_000; # size of files containing item data in JSON format
my $OUT_CHUNK_SIZE= 640_000_000; # size of files containing item data in JSON format
my $MAX_INPUT_LINES= undef;
# my $MAX_INPUT_LINES= 100_000; # for debugging to limit processing time

my $lang= 'de';
my $seq= 'a';
my $date= '2016-08-01'; # maybe a config file is in order to set up the defaults...
my ($fnm, $data_dir, $out_dir)= Wiktionary::Utils::get_paths ($lang, $date, $seq);
my $upd_paths= 0;

my $fo_compress= 2;
# 0..don't compress at all
# 1..compress output stream by piping into gzip; DO NOT USE
# 2..compress individual records using Compress::Zlib::compress()

binmode (STDOUT, ':utf8');

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
    elsif ($an eq 'lang') { $lang= $av || shift (@ARGV); $upd_paths= 1; }
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

($fnm, $data_dir, $out_dir)= Wiktionary::Utils::get_paths ($lang, $date, $seq) if ($upd_paths);

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
Wiktionary processor

date: $date
dump file name: $fnm
data dir: $data_dir
item dir: $out_dir
start_time: $ts_start
-----------
EOX

analyze_wiktionary_dump ($fnm);

exit(0);

sub analyze_wiktionary_dump
{
  my $fnm= shift;

  open (DIAG, '>:utf8', '@diag') or die;

  local *FI= wkutils::open_input($fnm);

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
  my @cols1= qw(line pos fo_count fo_pos_beg fo_pos_end id ns rev_id rev_sha1 title);
  print FO_ITEMS join ($TSV_SEP, @cols1), "\n";
  autoflush FO_ITEMS 1;

  my $fo_rec= new FDS('out_pattern' => "${out_dir}/wkt%05d");
  # $fo_rec->set (compress => 0, out_extension => '');
  my $fo_count= $fo_rec->open();
  my $fo_pos= 0;

  my $debug= 0;
  my $pos;
  my $state= 0;
  my %ns;
  my @lines;
  my %frame;
  my @text;
  my $cnt_ATTN= 0;
  my $debug_item= 0;
  LINE: while (1)
  {
    $pos= tell(FI);
    my $l= <FI>;
    last unless (defined ($l));

    if ($state == 0 # only when the <page> is closed
        && $fo_pos >= $OUT_CHUNK_SIZE)
    {
      $fo_count= $fo_rec->open();
      $fo_pos= 0;
    }
    $fo_pos= $fo_rec->tell();

    $line++;
    print join (' ', $line, $pos, $fo_count, $fo_pos), "\n" if (($line % 100_000) == 0);

    my $flush= 0;
    chomp ($l);

    print ">> [$state] [$l]\n" if ($debug > 1);
    if ($state == 0)
    {
      if ($l =~ m#^\s*<namespace key="([\-\d]+)" case="([^"]+)">([^"]*)</namespace>#)
      {
        my $ns= { ns_id => $1, ns_name => $3, ns_case => $2 };
        $ns{$ns->{ns_id}}= $ns;
      }
      elsif ($l =~ m#^\s*<page>#)
      {
        # print ">>> PAGE\n";
        $state= 1;
        @lines= ( $l );
        %frame= ( 'line' => $line, 'pos' => $pos, fo_count => $fo_count, fo_pos_beg => $fo_pos );
      }
    }
    elsif ($state == 1)
    {
      push (@lines, $l);
      if ($l =~ m#^\s*</page>#)
      {
        $state= 0;
        $flush= 1;
      }
      elsif ($l =~ m#^\s*<revision>#)
      {
        # print ">>> REVISION\n";
        $state= 2;
        @text= ();
      }
      elsif ($l =~ m#^\s*<(title|ns|id)>([^<]+)</.+>#)
      {
        $frame{$1}= $2;
      }
    }
    elsif ($state == 2)
    {
      push (@lines, $l);
      if ($l =~ m#^\s*</revision>#)
      {
        $state= 1;
      }
      elsif ($l =~ m#^\s*<text xml:space="preserve">(.*)#) # TODO: check for other <text> tags
      {
        my $t= $1;
        # print ">>> TEXT\n";
        $state= ($t =~ s#</text>##) ? 2 : 3;
        @text= ( $t );
      }
      elsif ($l =~ m#^\s*<text(.*)>#) # TODO: check for other <text> tags
      {
        print "ATTN: strange text-tag: [$l] title=[$frame{title}]\n";
        $cnt_ATTN++;
        $debug_item= 1;
      }
      elsif ($l =~ m#^\s*<(id|sha1)>([^<]+)</.+>#)
      {
        $frame{'rev_'. $1}= $2;
      }
    }
    elsif ($state == 3) # note: there could be <text>...</text> in a single line
    {
      push (@lines, $l);
      if ($l =~ m#^(.*)</text>$#)
      {
        push (@text, $1); # line-fragment!
        $state= 2;
      }
      else
      {
        push (@text, $l); # $line
      }
    }

    if ($flush)
    {
      $fo_rec->print (join ("\n", @lines));

      $frame{fo_pos_end}= $fo_rec->tell();

      if ($debug > 1 || $debug_item)
      {
        print "="x72, "\n";
        print __LINE__, " frame: ", Dumper(\%frame);
        print __LINE__, " text: ", Dumper(\@text);
        print __LINE__, " lines: ", Dumper (\@lines);
        print "="x72, "\n";

        $debug_item= 0;
      }

      print FO_ITEMS join ($TSV_SEP, map { $frame{$_} } @cols1), "\n";

      # statistics
      $ns{$frame{ns}}->{use_count}++;

      last if (defined ($MAX_INPUT_LINES) && $line > $MAX_INPUT_LINES);
    }
  }

  my $fnm_ns_json= join ('/', $data_dir, 'namespaces.json');
  my $fnm_ns_csv= join ('/', $data_dir, 'namespaces.csv');
  print "saving namespaces to [$fnm_ns_json]\n";
  Util::JSON::write_json_file ($fnm_ns_json, \%ns);

  # BUG: somehow $ns{'0'} ends up as $ns{''}; the counter seems to be right ...
  my @ns= map { $ns{$_} } sort { $a <=> $b } keys %ns;
  my $csv= new Util::Simple_CSV ('separator' => "\t", 'no_array' => 1);
  $csv->define_columns (qw(ns_id use_count ns_case ns_name));
  $csv->{data}= \@ns;
  $csv->save_csv_file(filename => $fnm_ns_csv);

  print "Attention-Count: $cnt_ATTN\n";

  1;
}
