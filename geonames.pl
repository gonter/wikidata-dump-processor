#!/usr/bin/perl

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;

my $dir= shift (@ARGV) || 'data/2019-08-05a';

my $data= {};

read_tsv($data, join('/', $dir, 'P1566.tsv'), 'P1566', 0);
read_tsv($data, join('/', $dir, 'P227.tsv'), 'P227', 1);

write_data($data, join('/', $dir, 'wdq-geonames-gnd.tsv'));

exit(0);

sub write_data
{
  my $data= shift;
  my $fnm= shift;

  open (FO, '>:utf8', $fnm) or die "can't write $fnm";
  print "writing $fnm\n";

  my @what= qw(P1566 P227); my @fields= qw(lang label val);
  my @columns= qw(id);
  foreach my $what (@what)
  {
    push (@columns, map { join('_', $what, $_) } @fields);
  }

  print FO join ("\t", @columns), "\n";

  ID: foreach my $id (sort keys %$data)
  {
    my $d= $data->{$id};

    next ID unless (exists($d->{P227}));

    my @vals= $id;
    foreach my $what (@what)
    {
      my $w= $d->{$what};
      push (@vals, map { $w->{$_} } @fields);
    }

    print FO join ("\t", @vals), "\n";
  }
  close (FO);
}

sub read_tsv
{
  my $data= shift;
  my $tsv= shift;
  my $what= shift;
  my $only_matching= shift;

  open (FI, '<:utf8', $tsv) or die "can't read $tsv";
  my $columns= <FI>; # TODO: chop columns and prepare index to match column numbers later on
  print "reading $tsv\n";
  my $count= 0;
  LINE: while (<FI>)
  {
    chop;
    my @f= split("\t");

    my $id= $f[5];
    next LINE if ($only_matching && !exists($data->{$id}));

    $data->{$id}->{$what}=
    {
      lang  => $f[12],
      label => $f[13],
      val   => $f[14],
    };
    $count++;
  }

  close(FI);

  $count;
}

