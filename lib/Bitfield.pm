package Bitfield;

use strict;

my @bits= (0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80);

sub new
{
  my $class= shift;
  bless [], $class;
}

sub update
{
  my $bf= shift || new Bitfield;
  my $ids= shift;
  my $offset= shift || 0;

  foreach my $id (@$ids)
  {
    my $num= substr($id,1);

    my $byte= int($num/8);
    my $bit= $num%8;
    $bf->[$byte] |= $bits[$bit];
  }

  $bf;
}

sub load_ids
{
  my $self= shift || new Bitfield;
  my $fnm= shift;
  my $offset= shift;

  open (FI, '<:utf8', $fnm) or die;
  my @data;
  while (<FI>)
  {
    chop;
    push (@data, $_);
  }
  close (FI);

  $self->update(\@data, $offset);

  $self;
}

sub write
{
  my $self= shift or die;
  my $fnm= shift;

  open (FO, '>:raw', $fnm) or die;
  my $buffer;
  foreach my $byte (@$self)
  {
    $buffer .= pack('C', $byte);
  }
  print FO $buffer;
  close (FO);
}

sub load
{
  my $self= shift;
  my $fnm= shift;

  my $class;
  $class= $self if (ref($self) eq '');

  my @st= stat($fnm) or die;
  my $buffer;
  open (FI, '<:raw', $fnm) or die;
  sysread(FI, $buffer, $st[7]);
  my @data= unpack('C*', $buffer);
  $self= \@data;
  bless($self, $class);
}

sub get_ids
{
  my $self= shift;
  my $prefix= shift;

  my $byte_idx= 0;
  my @data= ();
  foreach my $byte (@$self)
  {
    if ($byte)
    {
      for (my $idx= 0; $idx < 8; $idx++)
      {
        my $bit= $bits[$idx];
        if ($byte & $bit)
        {
          my $id= $prefix . ($byte_idx*8 + $idx);
          push (@data, $id);
        }
      }
    }

    $byte_idx++;
  }

  (wantarray) ? @data : \@data;
}

1;

__END__

