#!/usr/bin/perl

use strict;

use JSON;

use Data::Dumper;
$Data::Dumper::Indent= 1;
use FileHandle;

binmode( STDOUT, ':utf8' ); autoflush STDOUT 1;
binmode( STDERR, ':utf8' ); autoflush STDERR 1;
binmode( STDIN,  ':utf8' );

use lib 'lib';
use WikiData::Utils;
use WikiData::Property::Filter;

use FDS;

my @authctrl= qw(P213 P214 P227 P244 P496);
my @extract= ('id', @authctrl, qw(P19 P20 P21));
my @labels= (@extract, qw(tlt_en tlt_de tlt_fr tlt_it P569x P570x));

my $TSV_SEP= "\t";
# my $MAX_INPUT_LINES= undef;
my $MAX_INPUT_LINES= 1_000; # for debugging to limit processing time; TODO: add commandline option

my $seq= 'a';
my $date= '2016-12-19'; # maybe a config file should be used to set up the defaults...
my ($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq);
my $upd_paths= 0;


my @PARS= ();
while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PARS, @ARGV); @ARGV=(); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($an, $av)= split ('=', $1, 2);
    print "an=[$an] av=[$av]\n";

       if ($an eq 'date') { $date= $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'seq')  { $seq=  $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'max-lines') { $MAX_INPUT_LINES=  $av || shift (@ARGV); }
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

($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq) if ($upd_paths);
my $fnm_authctrl_json= join ('/', $data_dir, 'authctrl.json');
my $fnm_authctrl_tsv=  join ('/', $data_dir, 'authctrl.tsv');
my $fnm_diag=  join ('/', $data_dir, '@wdq3.diag');

open (DIAG, '>:utf8', $fnm_diag); # or be grumpy about that

parse_authctrl($fnm_authctrl_json, $fnm_authctrl_tsv);

close (DIAG);

exit (0);

sub usage
{
  system ('perldoc', $0);
  exit;
}

sub parse_authctrl
{
  my $fnm_in= shift;
  my $fnm_out= shift;

  my $running= 1;
  $SIG{INT}= sub { $running= 0; };

  # local *FI= wkutils::open_input($fnm);
  if ($fnm_in =~ /\.gz$/)
  {
    open (FI, '-|', "gunzip -c '$fnm_in'") or die "can't gunzip [$fnm_in]";
  }
  # elsif bunzip ... see wkt1
  else
  {
    open (FI, '<:utf8', $fnm_in) or die "can't read [$fnm_in]";
  }

  open (FO, '>:utf8', $fnm_out) or die "cant' write to [$fnm_out]\n";
  print FO join ("\t", @labels), "\n";
  my $fo_lines= 0;

  my $line= 0;
  my $t_start= time();
  my %p31;

  <FI>;
  my $pos;
  LINE: while ($running)
  {
    $pos= tell(FI);
    my $l= <FI>;
    last unless (defined ($l));

    $line++;
    print join (' ', $line, $pos), "\n" if (($line % 10_000) == 0);

    my $le= chop ($l);
    if ($l eq '[' || $l eq ']')
    {
      print "[$line] [$pos] skipping array bracket: $l\n";
      # $pos= tell(FI);
      next LINE;
    }

    my $sx= chop ($l); $l .= $sx if ($sx ne ',');

    # print "[$line] [$pos] [$l]\n";
    my $j;
    eval { utf8::upgrade($l); $j= decode_json ($l); }; 
    if ($@)
    {
      print "[$line] [$pos] ERROR=[", $@, "] line=[$l]\n";
      print DIAG "[$line] [$pos] ERROR=[", $@, "] line=[$line]\n";
      # $pos= tell(FI);
      next LINE;
    }

    # item is now in focus...
    my ($p31)= map { $j->{$_} } qw(P31);
    $p31{$p31}++;

    if ($p31 eq 'Q5')
    {
      # print __LINE__, " j: ", main::Dumper ($j);
      my @d= get_pers_data($j);
      # print __LINE__, " d: ", main::Dumper (\@d);

      print FO join ("\t", @d), "\n";
      $fo_lines++;
    }

  }

  close (FO);

  print DIAG __LINE__, " P31: ", main::Dumper(\%p31);

  $fo_lines;
}

sub get_pers_data
{
  my $j= shift;

  my @values= map { $j->{$_} } @extract;

  my ($tlt_l, $p569, $p570)= map { $j->{$_} } qw(tlt_l P569 P570);
  push (@values, map { $tlt_l->{$_} } qw(en de fr it));

  push (@values, get_time($p569), get_time($p570));

  @values;
}

sub get_time
{
  my $s= shift;

  # print "s=[$s]\n";
  return '' unless ($s);

  my $j= decode_json ($s);
  $j->{time};
}

