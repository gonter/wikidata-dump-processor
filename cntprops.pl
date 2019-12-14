#!/usr/bin/perl

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use lib 'lib';

use Bitfield;

my $data_dir= shift(@ARGV) || 'latest';

my $fnm_props= "data/$data_dir/props.csv";
my $fnm_items= "data/$data_dir/items.csv";
my $MAX_USE_CNT=  800000;
# my $MIN_USE_CNT=  800000; # at about this count, the ids file is smaller than the associated bitfield

my $props_dir= "data/$data_dir/props";
mkdir ($props_dir) unless (-d $props_dir);

my $cnt_props_open= 0;

my $props= parse_props($fnm_props);
parse_items($fnm_items, $props);
write_props($props);

sub parse_items
{
  my $fnm= shift;
  my $props= shift;

  open (FI, '<:utf8', $fnm) or die;

  my $cols= <FI>; chop($cols);
  my $idx= 0;
  my %cols= map { $_ => $idx++ } split("\t", $cols);

  my ($idx_id, $idx_claims)= map { $cols{$_} } qw(id claims);

  open (FO_no_P31, '>:utf8', "data/$data_dir/no_P31.lst");
  my $item_cnt= 0;
  my $claim_cnt= 0;
  while (my $l= <FI>)
  {
    chop($l);
    $item_cnt++;

    my @f= split("\t", $l);
    my $id= $f[$idx_id];
    my $claims= $f[$idx_claims];
    my @claims= split(',', $claims);

    my $has_P31= 0;
    CLAIM: foreach my $claim (@claims)
    {
      $claim_cnt++;
      if ($claim eq 'P31')
      {
        $has_P31= 1;
        # next CLAIM;
      }

      my $prop= $props->{$claim};

      # print "id=[$id] claim=[$claim] prop: ", Dumper($prop);

      if ($prop->{use_cnt} < $MAX_USE_CNT)
      {
        push (@{$prop->{ids}}, $id);
      }
      else
      {
        local *FH= get_fh ($prop, $claim);
        print FH $id, "\n";
      }
    }

    print FO_no_P31 $id, "\n" unless ($has_P31);

    print scalar localtime(), " item: ", $item_cnt, " claims: ", $claim_cnt, "\n", if (($item_cnt % 100000) == 0);
  }

  print scalar localtime(), " item: ", $item_cnt, " claims: ", $claim_cnt, "\n", if (($item_cnt % 100000) == 0);

=begin comment

  open (FO_dump, '>:utf8', 'props_xxx.dump');
  print FO_dump "props: ", Dumper($props);

=end comment
=cut

}

sub write_props
{
  my $props= shift;

  PROP: foreach my $prop (sort keys %$props)
  {
    my $p= $props->{$prop};

    if (exists($p->{ids}) && @{$p->{ids}})
    {
      my $fnm_out= "data/$data_dir/props/$prop";
      unless (open (FO, '>:utf8', $fnm_out))
      {
        print "can't write [$fnm_out]\n";
        next PROP;
      }
      print "writing $fnm_out\n";
      foreach (@{$p->{ids}})
      {
        print FO $_, "\n";
      }
      close (FO);
    }
    elsif (exists ($p->{_fh}))
    {
      my $fnm_ids= $p->{_fnm};
      print "closing $fnm_ids \n";
      close ($p->{_fh});

      my $bf= new Bitfield;
      print "loading ids from $fnm_ids\n";
      $bf->load_ids($fnm_ids, 1);

      my $fnm_bf= $fnm_ids . '.bitfield';
      print "writing bitfield to $fnm_bf\n";
      $bf->write($fnm_bf);

    }
  }
}

sub get_fh
{
  my $prop= shift;
  my $claim= shift;

  return $prop->{_fh} if (exists ($prop->{_fh}));

  local *F;
  my $out_fnm= "data/$data_dir/props/$claim";
  unless (open (F, '>:utf8', $out_fnm))
  {
    print "can't write to [$out_fnm]\n";
    return undef;
  }

  $prop->{_fnm}= $out_fnm;
  $cnt_props_open++;
  print scalar localtime(), " opening $out_fnm; $cnt_props_open file currently open\n";

  $prop->{_fh}= *F;
}

sub parse_props
{
  my $fnm= shift;

  open (FI, '<:utf8', $fnm) or die;

  my $cols= <FI>; chop($cols);
  my $idx= 0;
  my %cols= map { $_ => $idx++ } split("\t", $cols);
  my ($idx_prop, $idx_use_cnt)= map { $cols{$_} } qw(prop use_cnt);

  my $total_cnt= 0;
  my %use_cnt;
  my %props;
  while (my $l= <FI>)
  {
    chop($l);
    my @f= split("\t", $l);
    my $prop= $f[$idx_prop];
    my $use_cnt= $f[$idx_use_cnt];

    # printf("%5s %10ld\n", $prop, $use_cnt);
    # push (@{$use_cnt{$use_cnt}}, $prop);

    $props{$prop}= { prop => $prop, use_cnt => $use_cnt };
    $total_cnt += $use_cnt;
  }
  close (FI);

  printf("%5s %10ld claims\n", 'TOTAL', $total_cnt);
  # print "use_cnt: ", Dumper(\%use_cnt);
  sleep(3);

  \%props;
}

