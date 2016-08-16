package wkutils;

sub open_input
{
  my $fnm= shift;

  local *FI;
  if ($fnm =~ /\.gz$/)
  {
    open (FI, '-|', "gunzip -c '$fnm'") or die "can't gunzip [$fnm]";
  }
  elsif ($fnm =~ /\.bz2$/)
  {
    open (FI, '-|', "bunzip2 -c '$fnm'") or die "can't bunzip2 [$fnm]";
  }
  else
  {
    open (FI, '<:utf8', $fnm) or die "can't read [$fnm]";
  }

  binmode (FI, ':utf8');
  *FI;
}

1;
