
package Wiktionary::Utils;

use strict;

# TODO: make reasonable defaults and a command line option
sub get_paths
{
  my $lang= shift;
  my $date= shift;
  my $seq= shift || 'a';

  if ($date =~ m#^(\d{4})-?(\d{2})\-(\d{2})$#)
  {
    my ($yr, $mon, $day)= ($1, $2, $3);
    my $d1= join ('-', $yr, $mon, $day. $seq);

    # my $fnm= join ('', 'dumps/', $lang, 'wiktionary-', $yr, $mon, $day, '-pages-meta-current.xml.bz2');
    my $fnm= sprintf ('dumps/%swiktionary-%04d%02d%02d-pages-meta-current.xml.bz2', $lang, $yr, $mon, $day);
    my $data_dir= join ('/', "wkt-$lang", $d1);
    my $out_dir= join ('/', "wkt-$lang", $d1, 'out');

    return ($fnm, $data_dir, $out_dir);
  }

  die "invalid date format";
}

1;
