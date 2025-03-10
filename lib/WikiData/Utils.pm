
package WikiData::Utils;

use strict;

# TODO:
# * make reasonable defaults and a command line option
# * Wiktionary::Utils is more or less the same thing with other defaults; *unify* these modules!

sub get_paths
{
  my $date= shift;
  my $seq= shift;

  if ($date =~ m#^(\d{4})-?(\d{2})\-(\d{2})$#)
  {
    $seq= 'a' unless (defined ($seq));

    my ($yr, $mon, $day)= ($1, $2, $3);
    my $d1= join ('-', $yr, $mon, $day. $seq);

    my $fnm= join ('', 'dumps/', $yr, $mon, $day, '.json.gz');
    my $data_dir= join ('/', 'data', $d1);
    my $out_dir=  join ('/', 'data', $d1, 'out');
    my $prop_dir= join ('/', 'data', $d1, 'props');

    return ($fnm, $data_dir, $out_dir, $prop_dir);
  }
  elsif ($date eq 'latest')
  {
    my $data_dir= join ('/', 'data', 'latest');
    my $out_dir=  join ('/', 'data', 'latest', 'out');
    my $prop_dir= join ('/', 'data', 'latest', 'props');

    return (undef, $data_dir, $out_dir, $prop_dir);
  }

  die "invalid date format";
}

1;
