
package PDS;

=head1 NAME

  PDS

=head1 DESCRIPTION

  paged data store  --  can be used to keep index items

=cut

use strict;

my %defaults=
(
    # design values
    page_size => 4*1024*1024,
    page_hdr_size => 1024,
    rec_size => 32,
    backing_file => 'PDS_backing.pages',

    # watermarks
    last_page_num => -1,
    highest_page_num => -1,

    # counter
    cnt_page_same => 0,
    cnt_page_next => 0,
    cnt_page_up   => 0,
    cnt_page_down => 0,

    # cache
    last_page => undef,

    # bitmaps
    page_skips => {}, # number of times a page was skipped to (non incremental next)
    page_hits  => [], # number of times a page was loaded!
);

sub new
{
  my $class= shift;

  my $self= {};
  bless $self, $class;
  $self->set(@_);

  foreach my $an (keys %defaults)
  {
    $self->{$an}= $defaults{$an} unless (exists ($self->{$an}));
  }

  # derived semi-constants
  $self->{recs_per_page}= int (($self->{page_size} - $self->{page_hdr_size}) / $self->{rec_size});
  # print "recs_per_page=[$recs_per_page]\n"; exit;

  local *FPDS;
  my $bf= $self->{backing_file};
  my $bf_mode= (-f $bf) ? '+<:raw' : '+>:raw';

  unless (open (FPDS, $bf_mode, $bf))
  {
    die "can not create paging backing file [$self->{backing_file}] in mode [$bf_mode]";
    # TODO: do not die here...
  }
  print "opened paging backing file [$self->{backing_file}] in mode [$bf_mode]\n";
  $self->{__FPDS__}= *FPDS;

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

sub store
{
  my $self= shift;
  my $rec_num= shift;
  my $b= shift;

  my ($pdsp, $rel_rec_num, $rel_rec_pos)= $self->get_page_by_rec_num ($rec_num);
  $pdsp->{dirty}->[$rel_rec_num]= $b;
}

sub retrieve
{
  my $self= shift;
  my $rec_num= shift;
  my $b= shift;

  my ($pdsp, $rel_rec_num, $rel_rec_pos)= $self->get_page_by_rec_num ($rec_num);
  return undef unless (defined ($pdsp));
  # print "pdsp: ", main::Dumper($pdsp);
  # print "pdsp: rec_num=[$rec_num] page_num=[$pdsp->{page_num}] rel_rec_num=[$rel_rec_num] rel_rec_pos=[$rel_rec_pos]\n";
  my $d= substr ($pdsp->{buffer}, $rel_rec_pos, $self->{rec_size});

  # print "d:\n"; main::hexdump ($d);

  $d;
}

sub get_page_by_rec_num
{
  my $self= shift;
  my $rec_num= shift;

  my ($rec_size, $last_page_num, $last_page)= map { $self->{$_} } qw(rec_size last_page_num $last_page);

  my $page_num= int ($rec_num * $rec_size / $self->{page_size});
  my $rel_rec_num= $rec_num % $self->{recs_per_page};

  my $rel_rec_pos= $self->{page_hdr_size} + $rel_rec_num * $rec_size;

  # print __LINE__, " rec_num=[$rec_num] page_num=[$page_num]\n";

  if ($page_num == $last_page_num)
  {
    $self->{cnt_page_same}++;
  }
  elsif ($page_num < $last_page_num)
  {
    print "page down: rec_num=[$rec_num] last=[$last_page_num] next=[$page_num]\n";

    $self->{cnt_page_down}++;
    $self->{page_skips}->{$page_num}++;

    $self->flush_page();
    $self->load_page($page_num);
    # $self->{last_page_num}= $page_num;

    # $self->{last_page}= $self->load_page ($page_num);

    # print_page_stats();
  }
  elsif ($page_num > $last_page_num)
  {
    if ($page_num > $last_page_num+1)
    { # jump somehwere else
      print "page up: rec_num=[$rec_num] last=[$last_page_num] next=[$page_num]\n";
      $self->{cnt_page_up}++;
      $self->{page_skips}->{$page_num}++;
    }
    else
    {
      # print "page next: rec_num=[$rec_num] last=[$last_page_num] next=[$page_num]\n";
      $self->{cnt_page_next}++;
    }

    $self->flush_page();
    $self->load_page($page_num);

    # $self->{last_page}= $self->load_page ($page_num);

    if ($page_num > $self->{highest_page_num})
    {
      # print "highst_page_num changes from $self->{highest_page_num} to $page_num\n";
      $self->{highest_page_num}= $page_num;
    }

      # print_page_stats();
      # print_stats();
  }
  else
  {
    die (__LINE__, " internal error");
  }

  return ($self->{last_page}, $rel_rec_num, $rel_rec_pos);
}

sub print_page_stats
{
  my $self= shift;

  print "page statistics: same=[$self->{cnt_page_same}] next=[$self->{cnt_page_next}]",
        " up=[$self->{cnt_page_up}] down=[$self->{cnt_page_down}]\n";
}

sub print_page_info
{
  my $self= shift;

  print "page_size=[$self->{page_size}]\n";
  print "recs_per_page=[$self->{recs_per_page}]\n";
  $self->print_page_stats();
  print "highest_page_num=[$self->{highest_page_num}]\n";

  my $ps= $self->{page_skips};
  print "page_skips: ", join (', ', map { $_. ' <= '. $ps->{$_}.'x' } sort keys %$ps), "\n";
}

sub load_page
{
  my $self= shift;
  my $page_num= shift;

  # print "loading page_num=[$page_num]\n";

  my $new_page=
  {
    'page_num' => $page_num,
    'page_pos' => my $page_pos= $page_num * $self->{page_size},
    'dirty' => [],
    'buffer' => '',
  };

  if ($self->{do_read} || defined ($self->{page_hits}->[$page_num]))
  {
    # print "TODO: loading page data page_num=[$page_num]\n";
    $self->{page_hits}->[$page_num]++;

    local *FPDS= $self->{'__FPDS__'};
    my $page_size= $self->{page_size};

    my $rc= seek(FPDS, $page_pos, 0);
    # print "seek: rc=[$rc]\n";
    my $new_buffer;
    my $bc= sysread(FPDS, $new_buffer, $page_size);
    unless ($bc == $page_size)
    {
      die "ERROR saving page page_num=[$page_num] bc=[$bc] page_size=[$page_size]\n";
    }
    $new_page->{buffer}= $new_buffer;
  }
  else
  {
    # print "TODO: create new page_num=[$page_num]\n";
    # print "total=[$cnt_total] highst_page_num changes from $highest_page_num to $page_num\n";

    # $self->{highest_page_num}= $page_num;
    $self->{page_hits}->[$page_num]= 1;
  }

  $self->{last_page}= $new_page;
  $self->{last_page_num}= $page_num;

  # print "page loaded: ", main::Dumper ($new_page);
  $new_page;
}

sub setup_header
{
  my $self= shift;
  my $page_num= shift;
  my $updated_records= shift;

  my $s= 'PDSP'
         . pack ('LLL', map { $self->{$_} } qw(page_size page_hdr_size rec_size))
         . pack ('LLL', $page_num, time (), $updated_records)
         . "\0"x($self->{page_hdr_size}-28);

  my $x= substr ($s, 0, $self->{page_hdr_size});

  # main::hexdump ($x);

  $x;
}

sub flush_page
{
  my $self= shift;

  my ($page, $page_num)= map { $self->{$_} } qw(last_page last_page_num);

  # print "flushing page_num=[$page_num]\n";
  return undef unless ($page_num >= 0 && defined ($page));

  # print "TODO: writing data page_num=[$page_num]\n";

  # page metrics
  my ($page_size, $recs_per_page, $rec_size, $page_hdr_size)=
     map { $self->{$_} } qw(page_size recs_per_page rec_size page_hdr_size);

  # page coordinates
  my @d= @{$page->{dirty}};
  my $b= $page->{buffer};

  # my $cnt_dirty= @d;
  # print "flush: page_num=[$page_num] cnt_dirty=[$cnt_dirty]\n";

  my $new_buffer= $self->setup_header($page_num, 0x12345678);
  # print "new_buffer length=[",length($new_buffer), "]\n";

  my ($cnt_dirty, $cnt_buffer, $cnt_filler)= (0, 0, 0);
  foreach (my $i= 0; $i < $recs_per_page; $i++)
  {
    my $s;
    my $origin= 'unknown';
    if (defined ($d[$i]))
    {
      $s= substr ($d[$i], 0, $rec_size);
      $origin= 'dirty';
      $cnt_dirty++;
    }
    else
    { # retrieve buffered data or initialize as zero
      my $rel_pos= $page_hdr_size + $i * $rec_size;
      $s= substr ($b, $rel_pos, $rec_size);

      if (defined ($s))
      {
        $origin= 'buffer';
        $cnt_buffer++;
      }
      else
      {
        $s= pack ('Z'.$rec_size, '');
        $origin= 'filler';
        $cnt_filler++;
      }
    }

    # printf ("flush record %9d: size=%d origin=%s\n", $i, length($s), $origin);
    # main::hexdump ($s);
    $new_buffer .= $s;
  }

  local *FPDS= $self->{'__FPDS__'};
  my $rc= seek(FPDS, $page->{page_pos}, 0);
  # print "seek: rc=[$rc]\n";
  my $bc= syswrite(FPDS, $new_buffer, $page_size);
  unless ($bc == $page_size)
  {
    print "ERROR saving page page_num=[$page_num] bc=[$bc] page_size=[$page_size]\n";
  }
  print "NOTE: saved page page_num=[$page_num] cnt_dirty=[$cnt_dirty] cnt_buffer=[$cnt_buffer] cnt_fillter=[$cnt_filler]\n";

  $self->{page}= undef;
  $self->{last_page_num}= -1;

  $page_num;
  # exit;
}

1;

__END__

=head1 BUGS

  find a better name...


