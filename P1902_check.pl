#!/usr/bin/perl

use strict;

use Util::Simple_CSV;
use Data::Dumper;
$Data::Dumper::Indent= 1;
$Data::Dumper::SortKeys= 1;

my $prop= shift(@ARGV) || 'P1902';
my $tsv_fnm= shift (@ARGV) || sprintf ('data/latest/props/%s.tsv', $prop);

my $tsv= Util::Simple_CSV->new( load => $tsv_fnm, separator => "\t" );

# print __LINE__, " tsv: ", Dumper($tsv);
my $rows= $tsv->data();
my @columns= @{$tsv->{columns}};
# print __LINE__, ' columns: ', join(' ', @columns), "\n"; exit;

my @value_counts;
my %cnt;
my @problems= ();
foreach my $row (@$rows)
{
  # print __LINE__, " row: ", Dumper($row);
  my ($id, $label, $values)= map { $row->{$_} } qw(id label values);
  my @values= split(' ', $values);

  $value_counts[@values]++;
  my $has_invalid= 0;
  my $has_undefined= 0;
  my $has_novalue= 0;

  foreach my $value (@values)
  {
    unless ($value =~ m#^[A-Za-z0-9]{22}$#)
    {
      if ($value eq 'undefined') { $has_undefined++ }
      elsif ($value eq 'novalue') { $has_novalue++ }
      else { $has_invalid++ };
    }
  }
  
  if ($has_invalid || $has_undefined)
  {
    $cnt{invalid}++   if ($has_invalid);
    $cnt{undefined}++ if ($has_undefined);
    $cnt{novalue}++   if ($has_novalue);
    push (@problems, $row);
  }
  else
  {
    $cnt{ok}++;
  }
}

print '='x72, "\n";
print "REPORT\n";
print '='x72, "\n";
print 'statistics: ', Dumper(\%cnt);
print "counters:\n",
my $i;
for ($i= 0; $i <= $#value_counts; $i++)
{
  printf ("%2d %6d\n", $i, $value_counts[$i]);
}
print '='x72, "\n";
print "problems:\n";
print join("\t", @columns), "\n";
foreach my $p (@problems)
{
  print join("\t", map { $p->{$_} } @columns), "\n";
}

