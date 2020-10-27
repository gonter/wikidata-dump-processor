#!/usr/bin/perl

use strict;

my $dir= shift(@ARGV);

unless ($dir =~ m#^data/\d{4}-\d{2}-\d{2}([a-z\d]+)$#)
{
  print "dir [$dir] not formed as expected\n";
  exit;
}

my $cmd= "( head -n1 $dir/items_unsorted.tsv ; ( tail -n +2 $dir/items_unsorted.tsv | sort '-t\t' -k6.2n ) ) > $dir/items.tsv";

print "cmd=[$cmd]\n";

my $start= scalar localtime();
my $end=   scalar localtime();

system ($cmd);
print "start: $start\n";
print "end:   $end\n";


