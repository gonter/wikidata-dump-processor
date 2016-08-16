
package WikiData::Utils;

use strict;

# TODO: make reasonable defaults and a command line option
sub get_paths
{
  my $date= shift;
  my $seq= shift || 'a';

  if ($date =~ m#^(\d{4})-?(\d{2})\-(\d{2})$#)
  {
    my ($yr, $mon, $day)= ($1, $2, $3);
    my $d1= join ('-', $yr, $mon, $day. $seq);

    my $fnm= join ('', 'dumps/', $yr, $mon, $day, '.json.gz');
    my $data_dir= join ('/', 'data', $d1);
    my $out_dir= join ('/', 'data', $d1, 'out');

    return ($fnm, $data_dir, $out_dir);
  }

  die "invalid date format";
}

1;
