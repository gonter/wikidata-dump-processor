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

my $dumps_source= 'https://dumps.wikimedia.org/other/wikidata/';
my $wget= '/usr/bin/wget';

my $seq= 'a';
my $date= '2017-04-10';
my $expected_size= 11605714337;

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

notify('starting wdq0 loop');

while (1)
{
  my $dumps= check();
  # print "dumps: ", Dumper ($dumps);
  foreach my $dump (@$dumps)
  {
    fetch_and_convert ($dump->{date}, $seq, $dump->{size});
  }

  my $sleep_time= 3600 + int(rand(3600));
  print scalar localtime (time()), " sleeping until ", scalar localtime (time()+$sleep_time), "\n";
  sleep ($sleep_time);
}

exit (0);

sub notify
{
  my $msg= shift;

  system (qw(notify-sms.pl gg-uni), $msg);
}

sub fetch_and_convert
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
    print "fetching stuff for $date\n";
    notify('wdq0: this is a test send from w4.urxn.at');
    my ($fetched, $dump_file)= fetch_dump ($date);

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

    notify ('wdq0: finished download, starting wdq1');
    my @cmd1= (qw(./wdq1.pl --date), $date);
    print "cmd1: [", join (' ', @cmd1), "]\n";
    system (@cmd1);

    notify ('wdq0: finished wdq1, starting wdq2');
    my @cmd2= (qw(./wdq2.pl --scan --date), $date);
    print "cmd2: [", join (' ', @cmd2), "]\n";
    system (@cmd2);

    notify ('wdq0: finished wdq2, starting wdq3');
    my @cmd3= (qw(./wdq3.pl --date), $date);
    print "cmd3: [", join (' ', @cmd3), "]\n";
    system (@cmd3);

    notify ('wdq0: finished wikidata conversion');
  }

}


sub fetch_dump
{
  my $d= shift;

  $d=~ s#\-##g;

  my $dump_file= $d.'.json.gz';
  my $l_dump_file= 'dumps/'. $dump_file;
  print "dump_file=[$dump_file] l_dump_file=[$l_dump_file]\n";

  unless (-f $l_dump_file)
  {
    my $dump_url= $dumps_source . $dump_file;
    my @cmd_fetch= ($wget, $dump_url, '-O'.$l_dump_file);
    print "cmd_fetch: [", join (' ', @cmd_fetch), "]\n";
    # return undef;
    # system (@cmd_fetch);
  }

  my @st= stat ($l_dump_file);
  my $fetched;
  if (@st)
  {
    $fetched= $st[7];
  }

  ($fetched, $dump_file);
}

sub check
{
  my $cmd_fetch= "$wget $dumps_source -O-";

  print "cmd_fetch=[$cmd_fetch]\n";
  open (LST, '-|', $cmd_fetch) or die "can't run $cmd_fetch";
  my @res;
  while (<LST>)
  {
    chop;
    if (m#<a href="((\d{4})(\d{2})(\d{2})\.json\.gz)">(\d{8}\.json\.gz)</a>\s+(\S+)\s+(\S+)\s+(\d+)#)
    {
      my ($f1, $year, $mon, $day, $f2, $xdate, $time, $size)= ($1, $2, $3, $4, $5, $6, $7, $8);
      # print "year=[$year] mon=[$mon] day=[$day] f1=[$f1] f2=[$f2] xdate=[$xdate] time=[$time] size=[$size]\n";
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

