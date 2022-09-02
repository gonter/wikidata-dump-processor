
package FDS;

=head1 NAME

  FDS

=head1 DESCRIPTION

  framed data store  --

=cut

use strict;

use Compress::Zlib;
# use IO::Compress::Gzip;

sub new
{
  my $class= shift;

  my $self=
  {
    '_FO'    => undef,
    '_open'  => 0,
    '_count' => 0,
    '_pos'   => 0,
    '_fnm'   => undef,

    'compress' => 2,
# 0..don't compress at all
# 1..compress output stream by piping into gzip; DO NOT USE
# 2..compress individual records using Compress::Zlib::compress()
    'out_extension' => '.cmp', # 1.. .gz
  };

  bless $self, $class;
  $self->set(@_);

  $self;
}

sub set
{
  my $self= shift;
  my %par= @_;

  foreach my $par (keys %par)
  {
    $self->{$par}= $par{$par};
  }
}

# BEGIN output transcription

sub close
{
  my $self= shift;

  if ($self->{_open})
  {
    # print FO_RECODED "]\n";
    close ($self->{_FO});
    $self->{_open}= 0;
  }
}

sub open
{
  my $self= shift;

  $self->close();

  my $fo_fnm= sprintf ($self->{out_pattern} . $self->{out_extension}, ++$self->{_count});
  local *FO_RECODED;

  if ($self->{compress} == 1)
  {
    open (FO_RECODED, '|-', "gzip -c >'$fo_fnm'") or die "can't write to [$fo_fnm]";
  }
  elsif ($self->{compress} == 2)
  {
    open (FO_RECODED, '>:raw', $fo_fnm) or die "can't write to [$fo_fnm]";
  }
  else
  {
    open (FO_RECODED, '>:utf8', $fo_fnm) or die "can't write to [$fo_fnm]";
  }

  $self->{_FO}= *FO_RECODED;
  $self->{_fnm}= $fo_fnm;
  $self->{_open}= 1;

  print scalar localtime(time()), " writing dumps to $fo_fnm\n";
  # print FO_RECODED "[\n";

  $self->{_count};
}

sub tell
{
  my $self= shift;

  $self->{_pos}= tell ($self->{_FO});
}

sub print
{
  my $self= shift;
  my $l= shift;

  my $px;
  local *FO= $self->{_FO};

  if ($self->{compress} == 2)
  {
    # binmode (FO, ':raw');
    utf8::encode($l);
    my $compressed= compress($l);
    # print __LINE__, " compressed=[$compressed]\n";
    $px= print FO $compressed;
  }
  else
  {
    $px= print FO $l, "\n";
  }

  $px;
}
# END output transcription

1;

