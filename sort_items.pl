#!/usr/bin/perl

use strict;

my $dir= shift(@ARGV);

unless ($dir =~ m#^data/\d{4}-\d{2}-\d{2}[a-z]$#)
{
  print "dir [$dir] not formed as expected\n";
  exit;
}

my $cmd= "( head -n1 $dir/items_unsorted.csv ; ( tail -n +2 $dir/items_unsorted.csv | sort '-t\t' -k6.2n ) ) > $dir/items.csv";

print "cmd=[$cmd]\n";

my $start= scalar localtime();
my $end=   scalar localtime();

system ($cmd);
print "start: $start\n";
print "end:   $end\n";


