#!/usr/bin/perl

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use FileHandle

binmode (STDOUT, 'utf8');

my %INVALID_DOI_PREFIX= map { $_ => 1 } qw(10.38 10.475 10.530 10.581);
$INVALID_DOI_PREFIX{'10.5072'}= 1; # DataCite Test prefix!

# TODO: this script should check Handle values as well as - more selectively - DOIs

my $property;
my $check_dois= 1;

if ($check_dois)
{
  $property= 'P356'; # regular DOI values
}
else
{
  $property= 'P1184'; # plain Handle values
}

my %broken_fixed_not_found= # trivially fixed handle values with a valid URL which is not found
(
  '10-1123/JSR.2014-0201' => { fixed => '10.1123/JSR.2014-0201', status => 'not found', url => 'http://journals.humankinetics.com/jsr-technical-reports/jsr-technical-reports/effect-of-a-high-intensity-intermittent-exercise-protocol-on-neurocognitive-function-in-healthy-adults-implications-for-return-to-play-management-after-sport-related-concussion' },
  '10-1123/JSR.2014-0309' => { fixed => '10.1123/JSR.2014-0309', status => 'not found', url => 'http://journals.humankinetics.com/jsr-technical-reports/jsr-technical-reports/interrater-and-intrarater-reliability-and-validity-of-3-measurement-methods-for-shoulder-position-sense' },
  '10-1123/JSR.2015-0054' => { fixed => '10.1123/JSR.2015-0054', status => 'not found', url => 'http://journals.humankinetics.com/jsr-in-press/jsr-in-press/the-development-and-reliability-of-a-simple-field-based-screening-tool-to-assess-for-scapular-dyskinesis' },
  # '' => { fixed => '', status => 'not found', url => '' },
);

my $fnm= "data/latest/${property}.csv";

parse_tsv ($fnm);

sub parse_tsv
{
  my $fnm= shift;

  open (FI, '<:utf8', $fnm) or die;
  print "reading [$fnm]\n";

  my %counter;
  my (@good, @broken, @bad);
  my %prefix;
  my $lines= 0;
  <FI>; # munge TSV header
  DOI: while (<FI>)
  {
    chop;
    $lines++;

    my @f= split (/\t/, $_);

    my $wd_id= $f[5];
    my $label= $f[13];
    my $hdl= $f[14];
    my ($prefix, $sfx)= split (/\//, $hdl, 2);
    
    # print "wd_id=[$wd_id] prefix=[$prefix] sfx=[$sfx]\n";

    my $type= 'none';

    if ($prefix =~ s#^(HDL|DOI):(\d+)#$2#i) # the resolver fixes that, but it's still wrong ...
    {
      $counter{prefix_fixed}++;
      push (@bad, [$wd_id, 'prefix_fixed',  $hdl, $label]);
    }

    # TODO: this applies to DOIs only, not to Handle values
    if ($check_dois)
    {
      unless ($prefix =~ m#^10\.[1-9]\d{3,4}$#)
      {
        $counter{broken_prefix}++;

        my $reason= 'broken_doi_prefix';
        $reason .= ' try 10.4067' if ($hdl =~ m#^/S\d{4}\-\d+$#);
        $reason .= ' try 10.1123' if ($prefix eq '10-1123');
        $reason .= ' try 10.4414' if ($hdl =~ m#^/SMW\.\d{4}.\d+$#);

        push (@broken, [$wd_id, $reason,  $hdl, $label]);
        next DOI;
      }
    }

    if (exists ($INVALID_DOI_PREFIX{$prefix}))
    {
      $counter{invalid_doi_prefix}++;
      push (@broken, [$wd_id, 'invalid_doi_prefix', $hdl, $label]);
      next DOI;
    }

    if ($sfx =~ m#\?#) # this is valid for DOIs, but the entries really are broken
    {
      $counter{broken_sfx}++;
      push (@broken, [$wd_id, 'broken_sfx', $hdl, $label]);
      next DOI;
    }
    elsif ($sfx =~ m#\-# && $sfx =~ m#\.# && $sfx =~ m#\/#)
    {
      $type= 'dash_dot_slash';
    }
    elsif ($sfx =~ m#\-# && $sfx =~ m#\/#)
    {
      $type= 'dash_slash';
    }
    elsif ($sfx =~ m#\-# && $sfx =~ m#\.#)
    {
      $type= 'dash_dot';
    }
    elsif ($sfx =~ m#\.# && $sfx =~ m#\/#)
    {
      $type= 'dot_slash';
    }
    elsif ($sfx =~ m#\.#)
    {
      $type= 'dot';
    }
    elsif ($sfx =~ m#\-#)
    {
      $type= 'dash';
    }
    elsif ($sfx =~ m#\/#)
    {
      $type= 'slash';
    }

    $counter{$type}++;
    $prefix{$prefix}++;
    # print "wd_id=[$wd_id] type=[$type] prefix=[$prefix] sfx=[$sfx]\n";

    push (@good, [$wd_id, 'good',  $hdl, $label]) if ($type =~ /slash/);

    # last if ($lines >= 100_000);
  }
  close (FI);

  # print "prefix: ", Dumper (\%prefix);
  $counter{prefixes}= save_prefix_stats (\%prefix);

  if (@good)
  {
    # print "good: ", Dumper (\@good);
    $counter{good_handle}= save_broken_dois (\@good, "html/${property}-good.html");
  }

  if (@broken)
  {
    # print "broken: ", Dumper (\@broken);
    $counter{broken_handle}= save_broken_dois (\@broken, "html/${property}-broken.html");
  }

  if (@bad)
  {
    # print "bad: ", Dumper (\@bad);
    $counter{bad_handle}= save_broken_dois (\@bad, "html/${property}-bad.html");
  }

  print "counter: ", Dumper (\%counter);
}

sub save_broken_dois
{
  my $broken_doi_list= shift;
  my $fnm_html= shift || "html/${property}-broken.html";
  
  open (FO_HTML, '>:utf8', $fnm_html) or die;
  print "writing broken DOIs to [$fnm_html]\n";

  print FO_HTML <<EO_HTML;
<html>
<head>
<meta charset="UTF-8">
</head>
<body>
<table border=1>
<tr>
  <th>WikiData_ID</th>
  <th>reason</th>
  <th>DOI</th>
  <th>label</th>
</tr>
EO_HTML

  my $count= 0;
  foreach my $item (@$broken_doi_list)
  {
    my ($wd_id, $reason, $hdl, $label)= @$item;
    my $wd_url= sprintf ("https://www.wikidata.org/wiki/%s", $wd_id);

    my $hdl_url= sprintf ("http://hdl.handle.net/%s?auth=checked&noredirect=checked&ignore_aliases=checked", $hdl);

      print FO_HTML <<EO_HTML;
<tr>
  <td><a href="$wd_url" target="WD">$wd_id</a></td>
  <td>$reason</td>
  <td><a href="$hdl_url" target="HDL">$hdl</a></td>
  <td>$label</td>
</tr>
EO_HTML

    $count++;

    last if ($count >= 25_000);
  }

  print FO_HTML <<EO_HTML;
</table>
</body>
</html>
EO_HTML

  close (FO_HTML);

  $count;
}

sub save_prefix_stats
{
  my $prefix_list= shift;
  my $fnm_html= shift || "html/${property}-prefix-stats.html";

  open (FO_HTML, '>:utf8', $fnm_html) or die;
  print "writing prefix statistics to [$fnm_html]\n";

  print FO_HTML <<EO_HTML;
<html>
<head>
<meta charset="UTF-8">
</head>
<body>
<table border=1>
<tr>
  <th>count</th>
  <th>prefix</th>
</tr>
EO_HTML

  my @prefix_list= sort keys %$prefix_list;
  # print __LINE__, " prefix_list: ", Dumper(\@prefix_list);

  my $count= 0;
  my %cnt;
  foreach my $prefix (@prefix_list)
  {
    my $cnt= $prefix_list->{$prefix};
    push (@{$cnt{$cnt}}, $prefix);
  }
  # print __LINE__, " cnt: ", Dumper(\%cnt);
  my @cnt_list= sort { $b <=> $a } keys %cnt;
  # print __LINE__, " cnt_list: ", Dumper (@cnt_list);

  foreach my $cnt (@cnt_list)
  {
    my @prefix_cnt_list= sort @{$cnt{$cnt}};

    foreach my $prefix (@prefix_cnt_list)
    {
      my $na_url= sprintf ("http://hdl.handle.net/0.NA/%s?auth=checked&noredirect=checked&ignore_aliases=checked", $prefix);

      print FO_HTML <<EO_HTML;
<tr>
  <td>$cnt</td>
  <td><a href="$na_url" target="NA">$prefix</a></td>
</tr>
EO_HTML

      $count++;
    }
  }

  print FO_HTML <<EO_HTML;
</table>
</body>
</html>
EO_HTML

  close (FO_HTML);

  $count;
}


