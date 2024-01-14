#!/usr/bin/perl

use strict;

my $dir= shift(@ARGV);
my $sort_key= '-k6.2n';

   if ($dir =~ m#^data/\d{4}-\d{2}-\d{2}([a-z\d]+)$#) {}
elsif ($dir =~ m#^wkt-.*/\d{4}-\d{2}-\d{2}([a-z\d]+)$#)
{
  $sort_key= '-k6n';
}
else
{
  print "dir [$dir] not formed as expected\n";
  exit;
}

my $cmd= "( head -n1 $dir/items_unsorted.tsv ; ( tail -n +2 $dir/items_unsorted.tsv | sort '-t\t' $sort_key ) ) > $dir/items.tsv";

print "cmd=[$cmd]\n";

my $start= scalar localtime();
my $end=   scalar localtime();

system ($cmd);
print "start: $start\n";
print "end:   $end\n";


