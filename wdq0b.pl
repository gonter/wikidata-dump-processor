#!/usr/bin/perl

use strict;

use FileHandle;

use Util::JSON;
use Net::fanout;

autoflush STDOUT 1;

my $wdq0_config= join('/', $ENV{HOME}, 'etc/wdq0.json');
my $cfg= Util::JSON::read_json_file($wdq0_config);

# my $date= '2025-07-30'; my $seq= 'a'; my $content= 'data';
my $date= '2025-08-06'; my $seq= 'l'; my $content= 'lexemes';

my $upd_paths= 0;
my $doit= 0;

my @PARS;
while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PARS, @ARGV); @ARGV=(); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($an, $av)= split ('=', $1, 2);
    print "an=[$an] av=[$av]\n";

       if ($an eq 'date') { $date= $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'seq')  { $seq=  $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'content') { $content= $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'doit') { $doit= 1 }
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

my $dir= sprintf("data/%s%s", $date, $seq);
my $data_dir= join ('', $date, $seq);

die ("output dir $dir already exists") if (-d $dir);

run (qw(./wdq1.pl --date), $date, '--seq', $seq, '--content', $content);
run ('./sort_items.pl', $dir);
run (qw(./wdq2.pl --scan --date), $date, '--seq', $seq);

if ($content eq 'data')
{
  run (qw(./wdq3.pl --date), $date, '--seq', $seq);
  run ('./geonames.pl', $dir);
  run ('./cntprops.pl', $data_dir);
  run (qw(rm data/latest));
  run ('ln', '-s', $data_dir, 'data/latest');
}

exit(0);

sub run
{
  my @cmd= @_;

  notify(join(' ', @cmd));
  system (@cmd) if ($doit);
}

sub notify
{
  my $msg= shift;

  print __LINE__, ' ', scalar localtime(time()), ' ', $msg, "\n";
  my $fanout= Net::fanout->new($cfg->{fanout});
  $fanout->announce($cfg->{notify_channel}, $msg);
}

