#!/usr/bin/perl

use strict;

my @MONs= qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
my $mon= 1;
my %MONs= map { $_ => $mon++ } @MONs;

## configuration
my $max_age= 14400; # 4 hrs should do it
my $base_url= 'https://dumps.wikimedia.org/other/wikibase/wikidatawiki/';
my $fnm= 'index.html';
my $nx_fnm= '/var/lib/prometheus/node-exporter/wdq_fetcher.prom';
chdir ('dumps') or die;

my $now= time();
my $now_ts= scalar localtime ($now);
srand($$^$now);

my $wdq_fetcher_changes= 0;
my @st= stat($fnm);
if (@st == 0 || $st[9] + $max_age + int(rand(100)) <= time())
{
  my @cmd= ('wget', $base_url, '-O', $fnm);
  system (@cmd);
}

# rcsdiff index.html | perl -ne 'print join(" ", $3, $4, $5, $2, $1). "\n" if (m#^>.*>(latest-(.*)\.json.bz2)</a>\s*(\S+)\s+(\S+)\s+(\S+)#)'
# wget https://dumps.wikimedia.org/other/wikibase/wikidatawiki/latest-all.json.bz2 -O 2025-10-18_latest-all.json.bz2 

my @lines= split("\n", `rcsdiff $fnm`);
my @report;
my %commands;
my @unknown;
my $cnt_ignored_formats= 0;
my $rcsdiff_line_count= 0;
my @subdirs;
while (my $l= shift (@lines))
{
  $l=~ s/\r//g;
  # print __LINE__, " rcsdiff=[", $l, "]\n";
  $rcsdiff_line_count++;
  if ($l =~ m#^> <a href.+>(latest-(.*)\.json.bz2)</a>\s*(\S+)\s+(\S+)\s+(\S+)#)
  {
    my ($fnm, $type, $date, $time, $size)= ($1, $2, $3, $4, $5);
    my $date_iso= date_to_iso($date);
    push (@report, sprintf("new: %s %s %11d %7s %s", $date_iso, $time, $size, $type, $fnm));
    if ($size > 400_000_000) # lexemes are less than 1/2 GB while "all" is apporaching 100 GB
    {
      $commands{$type}= ['wget', $base_url.$fnm, '-O', $date_iso.'-'.$fnm];
    }
  }
  elsif ($l =~ m#^[<>] <a href.+>(\d{8})/</a>\s*(\S+)\s+(\S+)\s+-#)
  {
    my ($dir, $date, $time)= ($1, $2, $3);
    push (@subdirs, { dir => $dir, date => $date, time => $time } );
  }
  elsif ($l =~ m#^[<>] <a href.+>(latest-(all|lexemes)\.(nt|ttl)\.(bz2|gz))</a>\s*(\S+)\s+(\S+)\s+(\S+)#
         || $l =~ m#^[<>] <a href.+>(latest-(.*)\.json.gz)</a>\s*(\S+)\s+(\S+)\s+(\S+)#
        )
  {
    $cnt_ignored_formats++;
  }
  elsif ($l eq '---') {}
  elsif ($l =~ m#^\d+(,\d+)?c\d+(,\d+)?#) {}
  else { push (@unknown, $l); }

}
my $cnt_unknown_lines= @unknown;
print __LINE__, " rcsdiff: $rcsdiff_line_count lines processed; $cnt_ignored_formats formats ignored; $cnt_unknown_lines lines unknown\n";

if ($rcsdiff_line_count)
{
  my @commands;
  foreach my $type (qw(lexemes all))
  {
    push (@commands, $commands{$type}) if (exists($commands{$type}));
  }

  my @commit_message_parts; 
  if (@commands)
  {
    push (@commit_message_parts, 'updated dumps: '.join(' ', keys %commands));
  }

  push (@commit_message_parts, scalar @subdirs . ' subdirs ignored') if (@subdirs);
  push (@commit_message_parts, "$cnt_ignored_formats ignored formats") if ($cnt_ignored_formats);

  if (@commit_message_parts)
  {
    my $commit_message= join('; ', @commit_message_parts);
    unshift (@commands, ['ci', '-l', '-m'.$commit_message , $fnm]);
    $wdq_fetcher_changes= 1;
  }

  print_lines ('report', @report);
  print_lines ('unknown', @unknown);
  print_lines ('commands', map { join(' ', @$_) } @commands);

  print __LINE__, " fnm=[$fnm] rcsdiff_line_count=[$rcsdiff_line_count] wdq_fetcher_changes=[$wdq_fetcher_changes]\n";

  if (0 # better check it for some time
      && @commands && $cnt_unknown_lines == 0
     )
  {
    foreach my $command (@commands)
    { # ATTN: these are long running processes!
      system (@$command);
    }
  }
}

# write metrics
open (NX, '>:utf8', $nx_fnm) or die;
print __LINE__, " $now_ts writing metrics to [$nx_fnm]\n";
print NX <<"EOX";
# HELP wdq_fetcher_changes arcconf monitor noticed some problems
# TYPE wdq_fetcher_changes gauge
wdq_fetcher_changes $wdq_fetcher_changes
# HELP agent_last_run last time wdq_fetcher ran: $now_ts
# TYPE agent_last_run counter
agent_last_run{role="wdq_fetcher"} $now
EOX
close (NX);

sub date_to_iso
{
  my $date= shift;
  my ($day, $mon_name, $year)= split('-', $date);
  my $mon_num= $MONs{$mon_name};
  sprintf("%04d-%02d-%02d", $year, $mon_num, $day);
}

sub print_lines
{
  my $what= shift;
  if (@_)
  {
    print $what, ":\n";
    foreach (@_) { print $_, "\n" }
    print "\n";
  }
}

__END__

## Todo
- [ ] read configuration from file
- [ ] open a ticket or so, if something was downloaded

