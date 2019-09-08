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

use WikiData::Utils;
use Wiktionary::Utils;
use PDS;

my $data_dumps_source= 'https://dumps.wikimedia.org/other/wikidata/';
my $wkt_dumps_source= 'https://dumps.wikimedia.org/';
my $wget= '/usr/bin/wget';

my $seq= 'a';
my $date= '2017-04-10';
my $expected_size= 11605714337;

my $process_data_dumps= 1;

my $lang= undef;
my ($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq);
my $cmp_fnm_pattern= '%s/wdq%05d.cmp';

# my $op_mode= 'find_items';
my $op_mode= 'get_items';

my $upd_paths= 0;

autoflush STDOUT 1;

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
    elsif ($an eq 'lang') { $lang=  $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'scan')  { $op_mode= 'scan'; }
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

# notify('starting wdq0 loop');

while (1)
{
  if ($process_data_dumps)
  {
    my $dumps= check_data_dump();
    print "dumps: ", Dumper ($dumps);
    foreach my $dump (@$dumps)
    {
      fetch_and_convert_data_dump ($dump->{date}, $seq, $dump->{size});
    }
  }

  print " checking wiktionary dumps\n";
  foreach my $lang (qw(de en nl fr it))
  {
    print " checking wiktionary dump for lang=[$lang]\n";
    my @lang_dirs= check_wkt_all_dumps($lang);
    print __LINE__, " lang_dirs: ", join(' ', @lang_dirs), "\n";
    my $last= pop (@lang_dirs);
    my ($dir_url, $dump_file, $dump_date)= check_wkt_dump($lang, $last);
    fetch_and_convert_wkt_dump($lang, $dir_url, $dump_file, $dump_date);
  }
 
  my $sleep_time= 3600 + int(rand(3600));
  print scalar localtime (time()), " sleeping until ", scalar localtime (time()+$sleep_time), "\n";
  sleep ($sleep_time);
}

exit (0);

sub notify
{
  my $msg= shift;

  print "NOTIFY: [$msg]\n";
  system (qw(notify-sms.pl gg-uni), scalar localtime(time()), $msg);
  sleep(1);
}

sub fetch_and_convert_data_dump
{
  my $date= shift;
  my $seq= shift;
  my $expected_size= shift;

  my $data_dir= sprintf ("data/%s%s", $date, $seq);
  print "date=[$date] data_dir=[$data_dir]\n";

  if (-d $data_dir)
  {
    print "data_dir=[$data_dir] is already present\n";
  }
  else
  {
    print "fetching stuff for date=$date seq=$seq data_dir=[$data_dir]\n";
    notify("wdq0: about to fetch dump for $date");
    my ($fetched, $dump_file)= fetch_data_dump ($date);

    if ($fetched)
    {
      if ($fetched == $expected_size)
      {
        print "NOTE: fetched file seems good\n";
      }
      elsif ($fetched < $expected_size)
      {
        print "WARNING: fetch in progress? expected_size=[$expected_size] actual_size=[$fetched]; skipping...\n";
        return undef;
      }
      else
      {
        print "ERROR: fetched file too big? expected_size=[$expected_size] actual_size=[$fetched]";
        return undef;
      }
    }

    unless (defined ($dump_file))
    {
      print "ERROR: dump_file=[$dump_file] not available\n";
      return undef;
    }

    notify ("wdq0: finished download, size=$fetched, starting wdq1");
    my @cmd1= (qw(./wdq1.pl --date), $date);
    print "cmd1: [", join (' ', @cmd1), "]\n";
    system (@cmd1);

    my $dir= sprintf("data/%sa", $date);

    my @cmd1b= ('./sort_items.pl', $dir);
    print "cmd1b: [", join (' ', @cmd1b), "]\n";
    system (@cmd1b);

    notify ('wdq0: finished wdq1, starting wdq2');
    my @cmd2= (qw(./wdq2.pl --scan --date), $date);
    print "cmd2: [", join (' ', @cmd2), "]\n";
    system (@cmd2);

    notify ('wdq0: finished wdq2, starting wdq3');
    my @cmd3= (qw(./wdq3.pl --date), $date);
    print "cmd3: [", join (' ', @cmd3), "]\n";
    system (@cmd3);

    notify ('wdq0: finished wdq3, starting geonames');
    my @cmd4= ('./geonames.pl', $dir);
    print "cmd4: [", join (' ', @cmd4), "]\n";
    system (@cmd4);

    # TODO: add symlink
    system (qw(rm data/latest));
    system ('ln', '-s', join ('', $date, $seq), 'data/latest');

    notify ('wdq0: finished wikidata conversion');
  }

}

sub fetch_data_dump
{
  my $d= shift;

  $d=~ s#\-##g;

  my $dump_file= $d.'.json.gz';
  my $l_dump_file= 'dumps/'. $dump_file;
  print __LINE__, " dump_file=[$dump_file] l_dump_file=[$l_dump_file]\n";

  unless (-f $l_dump_file)
  {
    my $dump_url= $data_dumps_source . $dump_file;
    my @cmd_fetch= ($wget, $dump_url, '-O'.$l_dump_file);
    print "cmd_fetch: [", join (' ', @cmd_fetch), "]\n";
    sleep (60); # TODO: wait a little, lately we fetched 0 byte size files; this should be checked before starting the download
    # return undef;
    system (@cmd_fetch);
  }

  my @st= stat ($l_dump_file);
  my $fetched;
  if (@st)
  {
    $fetched= $st[7];
  }

  ($fetched, $dump_file);
}

sub check_data_dump
{
  my $cmd_fetch= "$wget $data_dumps_source -O-";

  print "cmd_fetch=[$cmd_fetch]\n";
  open (LST, '-|', $cmd_fetch) or die "can't run $cmd_fetch";
  my @res;
  LST: while (<LST>)
  {
    chop;
    if (m#<a href="((\d{4})(\d{2})(\d{2})\.json\.gz)">(\d{8}\.json\.gz)</a>\s+(\S+)\s+(\S+)\s+(\d+)#)
    {
      my ($f1, $year, $mon, $day, $f2, $xdate, $time, $size)= ($1, $2, $3, $4, $5, $6, $7, $8);
      print "year=[$year] mon=[$mon] day=[$day] f1=[$f1] f2=[$f2] xdate=[$xdate] time=[$time] size=[$size]\n";
      next LST if ($size <= 63);
      next LST if ($size <= 30_000_000_000);
      my $rec=
      {
        dump_file => $f1,
        date => join ('-', $year, $mon, $day),
        size => $size,
      };
      push (@res, $rec);
    }
  }

  (wantarray) ? @res : \@res;
}

sub check_wkt_all_dumps
{
  my $lang= shift;

  my $url= $wkt_dumps_source . $lang . 'wiktionary/';

  my $cmd_fetch= "$wget $url -O-";

  print "cmd_fetch=[$cmd_fetch]\n";
  open (LST, '-|', $cmd_fetch) or die "can't run $cmd_fetch";

  my @dates;
  LST: while (<LST>)
  {
    chop;

    # print __LINE__, " line=[$_]\n";

    push (@dates, $1) if ($_ =~ m#<a href="(\d{8})/"#);
  }

  @dates= sort @dates;
  (wantarray) ? @dates : \@dates;
}

sub check_wkt_dump
{
  my $lang= shift;
  my $date= shift;

  my $url= $wkt_dumps_source . $lang . 'wiktionary/'. $date . '/';

  my $cmd_fetch= "$wget $url -O-";

  print "cmd_fetch=[$cmd_fetch]\n";
  open (LST, '-|', $cmd_fetch) or die "can't run $cmd_fetch";

  my ($dump_file, $dump_date);
  LST: while (<LST>)
  {
    chop;

    # print __LINE__, " line=[$_]\n";

    # push (@dates, $1) if ($_ =~ m#<a href="(\d{8})/"#);

    if ($_ =~ m#<li class='done'>.*>(..wiktionary-(........)-pages-meta-current.xml.bz2)<#)
    {
      ($dump_file, $dump_date)= ($1, $2);
      print __LINE__, ">>> line=[$_]\n";
    }
  }

  print __LINE__, ">>> dump_file=[$dump_file]\n";
  ($url, $dump_file, $dump_date);
}

sub fetch_and_convert_wkt_dump
{
  my $lang= shift;
  my $dir_url= shift;
  my $dump_file= shift;
  my $dump_date= shift;

  my $dump_date2;
  if ($dump_date =~ m#^(\d{4})(\d\d)(\d\d)$#)
  {
    my ($year, $mon, $day)= ($1, $2, $3);
    $dump_date2= join('-', $year, $mon, $day)
  }
  else
  {
    print __LINE__, " dump_date=[$dump_date] malformed\n";
    return undef;
  }

  print __LINE__, " lang=[$lang] dir_url=[$dir_url] dump_file=[$dump_file] dump_date=[$dump_date]\n";
  my $l_dump_file= 'dumps/'. $dump_file;
  print __LINE__, " l_dump_file=[$l_dump_file]\n";

  unless (-f $l_dump_file)
  {
    my $dump_url= $dir_url . $dump_file;
    print __LINE__, " fetching  l_dump_file=[$l_dump_file]\n";

    notify("about to fetch wiktionary dump $dump_file");
    my @cmd_fetch= ($wget, $dump_url, '-O'.$l_dump_file);
    print "cmd_fetch: [", join (' ', @cmd_fetch), "]\n";
    sleep (60); # TODO: wait a little, lately we fetched 0 byte size files; this should be checked before starting the download
    # return undef;
    system (@cmd_fetch);
  }

  my $proc_seq= 'a';
  my $proc_dir= sprintf('wkt-%s/%s%s', $lang, $dump_date2, $proc_seq);
  print __LINE__, " proc_dir=[$proc_dir]\n";
  unless (-d $proc_dir)
  {
    notify("about to process wiktionary dump $dump_file");
    my @wkt_cmd= (qw(./wkt1.pl --lang), $lang, '--date', $dump_date2, '--seq', $proc_seq);
    print __LINE__, " wkt_cmd: ", join(' ', @wkt_cmd), "\n";
    system(@wkt_cmd);

    my @idx_cmd= ('./wdq2.pl', '--lang', $lang, '--date', $dump_date2, '--scan');
    print __LINE__, " idx_cmd: ", join(' ', @idx_cmd), "\n";
    system(@idx_cmd);
  }
}

