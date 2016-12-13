
package WikiData::Property::Filter;

my $TSV_SEP= "\t";

sub new
{
  my $class= shift;
  my %par= @_;

  my $obj= {};
  bless $obj, $class;
  $obj->set (%par);
  $obj->setup();

  $obj;
}

sub set
{
  my $obj= shift;
  my %par= @_;

  foreach my $par (keys %par)
  {
    if (defined ($par{$par}))
    {
      $obj->{$par}= $par{$par};
    }
    else
    {
      delete($obj->{$par});
    }
  }
}

sub setup
{
  my $obj= shift;

  my ($property, $cols, $label, $fnm_prop)= map { $obj->{$_} } qw(property cols label filename); 
  my $res= undef;

      if ($property =~ m#^P\d+$#)
      {
        if (defined ($fnm_prop))
        {
          local *FO_Prop;
          if (open (FO_Prop, '>:utf8', $fnm_prop))
          {
            print FO_Prop join ($TSV_SEP, @$cols), "\n" if (defined ($cols));
            print "writing filter [$property] [$label] to [$fnm_prop]\n";
            $obj->{'_FO'}= *FO_Prop;
            $res= 1;
          }
          else
          {
            print "can not write to [$fnm_prop]\n";
          }
        } # otherwise: do not write
      }
      else
      {
        print "ATTN: invalid property format [$property]; ignored\n";
        $res= -1
      }

  $res;
}

sub extract
{
  my $fp= shift;
  my $x= shift;

  my $y;
  _extract ($x, $fp->{'transform'});
}

sub _extract
{
  my $x= shift;
  my $transform= shift;

  if ($transform == 1 && ref ($x) eq 'HASH')
  {
    my $et;
    if ($x->{'entity-type'} eq 'item') { $et= 'Q'; }
    elsif ($x->{'entity-type'} eq 'property') { $et= 'P'; }
    $y= $et . $x->{'numeric-id'} if (defined ($et));
  }
  elsif (ref ($x) eq 'HASH')
  {
    $y= JSON::encode_json ($x);
  }
  else
  {
    $y= $x;
  }

  $y;
}

1;

__END__

