#!/usr/bin/perl

use strict;

use FileHandle;
use utf8;

binmode( STDOUT, ':utf8' ); autoflush STDOUT 1;
binmode( STDERR, ':utf8' ); autoflush STDERR 1;
binmode( STDIN,  ':utf8' );

use Data::Dumper;
$Data::Dumper::Indent= 1;
$Data::Dumper::Sortkeys= 1;

use PocketBase::API;
use Util::JSON;

my @MONs= qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
my $mon= 1;
my %MONs= map { $_ => $mon++ } @MONs;

## PocketBook configuration and API access
my $agent_config_file= 'wdq_fetcher.json'; # TODO: find a config file from default locations, e.g. ~/.config/ or ~/etc/ etc...
my $agent_cnf= Util::JSON::read_json_file ($agent_config_file);
my ($pb_name, $col_name)= map { $agent_cnf->{$_} } qw(PocketBase_name collection_name);
print __LINE__, " pb_name=[$pb_name] col_name=[$col_name]\n";
my $pb_cnf= $agent_cnf->{PocketBases}->{$pb_name};
my $pb_api= new PocketBase::API( config => $pb_cnf->{api_config} );
my ($code, $text, $result)= $pb_api->auth_with_password($col_name);
# print __LINE__, " auth_with_password: code=[$code] result: ", Dumper ($result);
print __LINE__, " auth_with_password: code=[$code]\n";

## more configuration
my $max_age= 14400; # 4 hrs should do it
my $base_url= 'https://dumps.wikimedia.org/other/wikibase/wikidatawiki/';
my $fnm= 'index.html';
my $nx_fnm= '/var/lib/prometheus/node-exporter/wdq_fetcher.prom';

my $x_flag= 0;
my $doit= 0;

my @PARS;
my $arg;
while (defined ($arg= shift (@ARGV)))
{
  utf8::decode($arg); # needed to process utf8 characters in commandline arguments

     if ($arg eq '-')  { push (@PARS, '-'); }
  elsif ($arg eq '--') { push (@PARS, @ARGV); @ARGV= (); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($opt, $val)= split ('=', $1, 2); 
    if ($opt eq 'help') { usage(); }
    elsif ($opt eq 'doit') { $doit= 1 }
    else { usage(); }
  }
  elsif ($arg =~ /^-(.+)/)
  {
    foreach my $opt (split ('', $1))
    {   
         if ($opt eq 'h') { usage(); exit (0); }
      elsif ($opt eq 'x') { $x_flag= 1; }
      else { usage(); }
    }   
  }
  else
  {
    push (@PARS, $arg);
  }
}

## setup
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
my %actions;
my @unknown;
my $cnt_ignored_formats= 0;
my $cnt_deletions= 0;
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
      my $pb_data= { dump_date => $date_iso, dump_type => $type, dump_size => $size, dl_status => 'pending' };
      $actions{$type}=
      {
        pb_data => $pb_data,
        command => ['wget', '--no-verbose', $base_url.$fnm, '-O', $date_iso.'_'.$fnm],
      };

      if ($doit)
      {
        my ($code_c, $text_c, $new_record)= $pb_api->create($col_name, $pb_data);
        print __LINE__, " create: code_c=[$code_c] new_record: ", Dumper($new_record);
        $pb_data->{id}= $new_record->{id};
      }
    }
  }
  elsif ($l =~ m#^< <a href.+>(latest-(.*)\.json.bz2)</a>\s*(\S+)\s+(\S+)\s+(\S+)#)
  {
    $cnt_deletions++;
  }
  elsif ($l =~ m#^[<>] <a href.+>(\d{8})/</a>\s*(\S+)\s+(\S+)\s+-#)
  {
    my ($dir, $date, $time)= ($1, $2, $3);
    push (@subdirs, { dir => $dir, date => $date, time => $time } );
  }
  elsif ($l =~ m#^[<>] <a href.+>(latest-(all|lexemes|truthy)\.(nt|ttl)\.(bz2|gz))</a>\s*(\S+)\s+(\S+)\s+(\S+)#
         || $l =~ m#^[<>] <a href.+>(latest-(.*)\.json.gz)</a>\s*(\S+)\s+(\S+)\s+(\S+)#
        )
  {
    $cnt_ignored_formats++;
  }
  elsif ($l eq '---') {}
  elsif ($l =~ m#^\d+(,\d+)?[acd]\d+(,\d+)?#) {}
  else { push (@unknown, $l); }

}
my $cnt_unknown_lines= @unknown;
print __LINE__, " rcsdiff: $rcsdiff_line_count lines processed; $cnt_ignored_formats formats ignored; $cnt_unknown_lines lines unknown\n";

print __LINE__, " actions hash: ", Dumper(\%actions);

if ($rcsdiff_line_count)
{
  my @actions;
  foreach my $type (qw(lexemes all))
  {
    push (@actions, $actions{$type}) if (exists($actions{$type}));
  }

  my @commit_message_parts; 
  if (@actions)
  {
    push (@commit_message_parts, 'updated dumps: '.join(' ', keys %actions));
  }

  push (@commit_message_parts, scalar @subdirs . ' subdirs ignored') if (@subdirs);
  push (@commit_message_parts, "$cnt_ignored_formats ignored formats") if ($cnt_ignored_formats);
  push (@commit_message_parts, "$cnt_ignored_formats deletions") if ($cnt_deletions);

  if (@commit_message_parts)
  {
    my $commit_message= join('; ', @commit_message_parts);
    print __LINE__, " commit_message=[$commit_message]\n";
    unshift (@actions, { command => ['ci', '-l', '-m'.$commit_message , $fnm] });
    $wdq_fetcher_changes= 1;
  }

  print_lines ('report', @report);
  print_lines ('unknown', @unknown);
  # print_lines ('commands', map { join(' ', @$_) } @actions);

  print __LINE__, " fnm=[$fnm] rcsdiff_line_count=[$rcsdiff_line_count] wdq_fetcher_changes=[$wdq_fetcher_changes]\n";
  # print __LINE__, " actions array: ", Dumper(\@actions);

  if ($doit
      && @actions
      && $cnt_unknown_lines == 0
     )
  {
    foreach my $action (@actions)
    { # ATTN: these are long running processes!
      print __LINE__, " action: ", Dumper($action);
      my ($command, $pb_data)= map { $action->{$_} } qw(command pb_data);

      if (defined ($pb_data))
      {
        my $upd1=
        { 
          dl_status => 'in_progress',
          dl_started => PocketBase::API::ts(),
        };
        if ($doit)
        {
          my ($code_u1, $text_u1, $result_u1)= $pb_api->update($col_name, $pb_data->{id}, $upd1);
          print __LINE__, " update: code_u1=[$code_u1] result_u1: ", Dumper($result_u1);
        }
      }

      system (@$command) if (defined ($command));

      if (defined ($pb_data))
      {
        my $upd2=
        { 
          dl_status => 'finished',
          dl_finished => PocketBase::API::ts(),
          prc_status => 'queued',
        };
        if ($doit)
        {
          my ($code_u2, $text_u2, $result_u2)= $pb_api->update($col_name, $pb_data->{id}, $upd2);
          print __LINE__, " update: code_u2=[$code_u2] result_u2: ", Dumper($result_u2);
        }
      }
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

