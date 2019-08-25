#!/usr/bin/perl

use strict;

my $dir= shift(@ARGV);

exit unless ($dir =~ m#^\d{4}-\d{2}-\d{2}[a-z]$#);

my $cmd= sprintf ("( head -n1 data/$dir/items_unsorted.csv ; ( tail -n +2 data/$dir/items_unsorted.csv | sort '-t\t' -k6.2n ) ) > data/$dir/items.csv", $dir);

print "cmd=[$cmd]\n";

my $start= scalar localtime();
my $end=   scalar localtime();

system ($cmd);
print "start: $start\n";
print "end:   $end\n";


