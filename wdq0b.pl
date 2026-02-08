#!/usr/bin/perl

use strict;

use FileHandle;

use PocketBase::API;
use Util::JSON;
use Net::fanout;

use Data::Dumper;
$Data::Dumper::Indent= 1;
$Data::Dumper::Sortkeys= 1;

autoflush STDOUT 1;

my $wdq0_config= join('/', $ENV{HOME}, 'etc/wdq0.json');
my $cfg= Util::JSON::read_json_file($wdq0_config);

# my ($date, $seq, $content)= ('2025-07-30', 'a', 'data');
my ($date, $seq, $content)= ('2025-08-06', 'l', 'lexemes');

my $upd_paths= 0;
my $doit= 0;
my $op_mode= 'process';
my $run_queued= 0;

my ($pb_name, $col_name, $pb_api, $job_id);

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
    # PocketBase
    elsif ($an eq 'list') { $op_mode= 'list'; }
    elsif ($an eq 'queued') { $op_mode= 'queued'; }
    elsif ($an eq 'run') { $run_queued= 1; }
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

if ($op_mode eq 'queued')
{
  my ($total_items, $items)= pb_show(prc_status => 'queued');
  print __LINE__, " total_items=[$total_items]\n";

  if ($run_queued && $total_items >= 1)
  {
    my $job= $items->[0];
    print __LINE__, " preparing job: ", Dumper($job);
    my ($id, $dd, $dt)= map { $job->{$_} } qw(id dump_date dump_type);

       if ($dt eq 'all')     { ($date, $seq, $content)= ($dd, 'a', 'data'); }
    elsif ($dt eq 'lexemes') { ($date, $seq, $content)= ($dd, 'l', 'lexemes'); }
    else { "die unknown dump_type=[$dt]"; }

    print __LINE__, " date=[$date] seq=[$seq] content=[$content]\n";
    my $upd1=
    { 
      prc_status => 'in_progress',
      prc_started => PocketBase::API::ts(),
    };
    print __LINE__, " update: id=[$id], upd1: ", Dumper($upd1);
    if ($doit)
    {
      $job_id= $id;
      $pb_api->update($col_name, $id, $upd1);
    }

    $op_mode= 'process';
  }
}
elsif ($op_mode eq 'list')
{
  pb_show();
}

if ($op_mode ne 'process')
{
  print "exiting ...\n";
  exit(0);
}

# $op_mode eq 'process'
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

if (defined ($pb_api) && defined ($col_name) && defined ($job_id))
{ # authentication may be necessary again, this job lasts for almost a week
  my ($code, $text, $result)= $pb_api->auth_with_password($col_name);
  # print __LINE__, " auth_with_password: code=[$code] result: ", Dumper ($result);
  print __LINE__, " auth_with_password: code=[$code]\n";

    my $upd2=
    { 
      prc_status => 'finished',
      prc_finished => PocketBase::API::ts(),
    };
    print __LINE__, " update: job_id=[$job_id], upd2: ", Dumper($upd2);
    if ($doit)
    {
      my $res= $pb_api->update($col_name, $job_id, $upd2);
      print __LINE__, " res: job_id=[$job_id]: ", Dumper($res);
    }
}

exit(0);

sub run
{
  my @cmd= @_;

  notify(join(' ', @cmd, 'started'));
  if ($doit)
  {
    system (@cmd);
    notify(join(' ', @cmd, 'finished'));
  }
}

sub notify
{
  my $msg= shift;

  print __LINE__, ' ', PocketBase::API::ts(), ' ', $msg, "\n";
  my $fanout= Net::fanout->new($cfg->{fanout});
  $fanout->announce($cfg->{notify_channel}, $msg);
}

sub connect_pocket_base
{
  ## PocketBook configuration and API access (from wdq_fetcher)
  my $agent_config_file= 'wdq_fetcher.json'; # TODO: find a config file from default locations, e.g. ~/.config/ or ~/etc/ etc...
  my $agent_cnf= Util::JSON::read_json_file ($agent_config_file);
  ($pb_name, $col_name)= map { $agent_cnf->{$_} } qw(PocketBase_name collection_name);
  print __LINE__, " pb_name=[$pb_name] col_name=[$col_name]\n";
  my $pb_cnf= $agent_cnf->{PocketBases}->{$pb_name};
  $pb_api= PocketBase::API->new( config => $pb_cnf->{api_config} );
  my ($code, $text, $result)= $pb_api->auth_with_password($col_name);
  # print __LINE__, " auth_with_password: code=[$code] result: ", Dumper ($result);
  print __LINE__, " auth_with_password: code=[$code]\n";

  return $pb_api;
}

sub pb_show
{
  my $what= shift;
  my $value= shift;

  my $pb_api= connect_pocket_base();
  print __LINE__, " pb_api=[$pb_api]\n";
  die "no pb_api" unless (defined ($pb_api));

  my @items= ();
  my $total_items;

  my @query_parameters;
  if (defined ($what))
  {
    push (@query_parameters, "filter=($what='$value')");
  }

  my $res= $pb_api->records($col_name, @query_parameters);
  print __LINE__, " res=[$res]: ", Dumper($res);
  # TODO: implement paging
  push (@items, @{$res->{items}});
  my $total_items= $res->{totalItems};

  ($total_items, \@items);
}

sub make_filter
{
  my %pars= shift;
}

