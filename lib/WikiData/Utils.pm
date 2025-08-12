
package WikiData::Utils;

use strict;

# TODO:
# * make reasonable defaults and a command line option
# * Wiktionary::Utils is more or less the same thing with other defaults; *unify* these modules!

sub get_paths
{
  my $date= shift;
  my $seq= shift;
  my $content= shift || 'data';

  $content= 'all' if ($content eq 'data');

  if ($date =~ m#^(\d{4})-?(\d{2})\-(\d{2})$#)
  {
    $seq= 'a' unless (defined ($seq));

    my ($yr, $mon, $day)= ($1, $2, $3);
    my $d1= join ('-', $yr, $mon, $day. $seq);

    # my $fnm= join ('', 'dumps/', $yr, $mon, $day, '.json.gz');
    my $fnm= join ('', 'dumps/', join ('-', $yr, $mon, $day), '_latest-'.$content.'.json.bz2'); # TODO: download link no longer contains the date:
    # https://dumps.wikimedia.org/other/wikibase/wikidatawiki/latest-all.json.bz2
    # TODO: date must be retrieved by the fetcher script from the index
    my $data_dir= join ('/', 'data', $d1);
    my $out_dir=  join ('/', 'data', $d1, 'out');
    my $prop_dir= join ('/', 'data', $d1, 'props');

    return ($fnm, $data_dir, $out_dir, $prop_dir);
  }
  elsif ($date eq 'latest' || $date eq 'lexemes')
  {
    my $data_dir= join ('/', 'data', $date);
    my $out_dir=  join ('/', 'data', $date, 'out');
    my $prop_dir= join ('/', 'data', $date, 'props');

    return (undef, $data_dir, $out_dir, $prop_dir);
  }

  die "invalid date format";
}

1;
