#!/usr/bin/perl

use strict;

use lib 'lib';

use FileHandle;

use JSON;
use Compress::Zlib;

use Util::Simple_CSV;
use Util::hexdump;

use Data::Dumper;
$Data::Dumper::Indent= 1;

use WikiData::Utils;

my $seq= 'a';
my $date= '2015-12-28';
my ($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq);

# my $op_mode= 'find_items';
my $op_mode= 'get_items';

my $upd_paths= 0;

autoflush STDOUT 1;

my @PARS= ();
while (my $arg= shift (@ARGV))
{
     if ($arg eq '--') { push (@PARS, @ARGV); @ARGV=(); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($an, $av)= split ('=', $1, 2);
    print "an=[$an] av=[$av]\n";

       if ($an eq 'date') { $date= $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'seq')  { $seq=  $av || shift (@ARGV); $upd_paths= 1; }
    elsif ($an eq 'scan')  { $op_mode= 'scan'; }
    else
    {
      usage();
    }
  }
  elsif ($arg =~ /^-(.+)/)
  {
    foreach my $flag (split('', $1))
    {
      usage();
    }
  }
  else { push (@PARS, $arg); }
}

# prepare items list
($fnm, $data_dir, $out_dir)= WikiData::Utils::get_paths ($date, $seq) if ($upd_paths);
my $fnm_items= join ('/', $data_dir, 'items.csv');

my $csv= new Util::Simple_CSV (separator => "\t");

local *FI;
my $fi_open;
(*FI, $fi_open)= $csv->open_csv_file ($fnm_items);
print "fi_open=[$fi_open]\n";
$csv->{'__FI'}= *FI;
$csv->load_csv_file_headings (*FI);

  my $rec_size= 32;
  my $pds= new PDS (rec_size => $rec_size);
  print "pds: ", Dumper ($pds);

if ($op_mode eq 'find_items')
{
  usage() unless (@PARS);
  find_items($csv, \@PARS);
}
elsif ($op_mode eq 'get_items')
{
  usage() unless (@PARS);
  get_items($csv, \@PARS);
}
elsif ($op_mode eq 'scan')
{
  scan_items($csv);
}
else { usage(); }

exit(0);

sub scan_items
{
  my $csv= shift;

  my $index= $csv->{'index'};
  print "index: ", Dumper ($index);
  my ($idx_id, $idx_fo_num, $idx_pos_beg, $idx_pos_end)= map { $index->{$_} } qw(id fo_count fo_pos_beg fo_pos_end);
  print "idx_id=[$idx_id] idx_fo_num=[$idx_fo_num] idx_pos_beg=[$idx_pos_beg] idx_pos_end=[$idx_pos_end]\n";

  my $columns= $csv->{'columns'};
  print "columns: ", Dumper ($columns);

  open (FO, '>:utf8', '@rogues.tsv');
  print FO join ("\t", @$columns), "\n";

  parse_idx_file ($csv->{'__FI'}, *FO, $idx_id, $idx_fo_num, $idx_pos_beg, $idx_pos_end);
}

sub parse_idx_file
{
  local *F_in= shift;
  local *F_rogues= shift;

  my $idx_id= shift;
  my $idx_fo_num= shift;
  my $idx_pos_beg= shift;
  my $idx_pos_end= shift;

# print "parse_idx_file\n";

  my $last_rec_num= 0; # there is no Q0

  my ($cnt_total, $cnt_invalid, $cnt_ordered, $cnt_rogue)= (0, 0, 0, 0);
  sub print_stats
  {
    print "statistics: total=[$cnt_total] invalid=[$cnt_invalid] ordered=[$cnt_ordered] rogue=[$cnt_rogue]\n";
  }

  # designed/optimized values
  # my $page_block_factor= 4; # this is guess work; try to avoid skipping around

  # my $page_size= 1024*$rec_size*$page_block_factor;
  # my $page_size= 4*1024*1024; # 4 MByte blocks, depending on fileystem layout
  # my $page_hdr_size= 1024;

  # statistics
  LINE: while (<F_in>)
  {
    chop;
    my @f= split ("\t");

    my $pos_idx= tell(F_in);
    $cnt_total++;

    my ($id, $f_num, $beg, $end)= map { $f[$_] } ($idx_id, $idx_fo_num, $idx_pos_beg, $idx_pos_end);

    my $rec_num;
    if ($id =~ m#^Q(\d+)$#)
    {
      $rec_num= $1;
    }
    else
    {
      print "unknown id format [$id] at line=[$cnt_total] offset=[$pos_idx] ", Dumper(\@f);
      $cnt_invalid++;
      next LINE;
    }

    my $rec_s= pack ('LLLLLLLL', $rec_num, $pos_idx, $f_num, $beg, $end, 0, 0, 0);
    $pds->store ($rec_num, $rec_s);

    next LINE;

=begin comment

That's not a good metric for estimating orderedness

statistics: total=[2700000] invalid=[0] ordered=[2700000] rogue=[0]
ROGUE: total=[2757300] last_rec_num=[12464824] rec_num=[12466169]

    # print "f: ", Dumper(\@f);
    if ($rec_num > $last_rec_num
        # && $rec_num <= $last_rec_num + 10_000
       )
    { # normal order
      $cnt_ordered++;
      print_stats() if (($cnt_ordered % 100_000) == 0);
      $last_rec_num= $rec_num;
    }
    else
    { # misordered items in the stream
      print "ROGUE: total=[$cnt_total] last_rec_num=[$last_rec_num] rec_num=[$rec_num]\n";
      $last_rec_num= $rec_num;
      # print F_rogues join ("\t", @f), "\n";
      $cnt_rogue++;
      print_stats() if (($cnt_rogue % 100) == 0);

      # last LINE if ($cnt_rogue >= 5);
    }

=end comment
=cut

    # print "id=[$id] rec_num=[$rec_num] pos_idx=[$pos_idx] f_num=[$f_num] beg=[$beg] end=[$end]\n";
  }

  $pds->flush_page();
  $pds->print_page_info();

  print_stats();
}

=head2 find_items($csv)

search for specific items and display their JSON structure

=cut

sub find_items
{
  my $csv= shift;
  my $pars= shift;

  unless (defined ($pars) && @$pars)
  {
    print "no items specified\n";
    return undef;
  }

  my $idx_id= $csv->{'index'}->{'id'};

  my %IDS= map { $_ => 1 } @$pars;
  print "IDS: ", Dumper (\%IDS);

  sub filter
  {
    my $row= shift;
  
    return (exists ($IDS{$row->[$idx_id]})) ? 1 : 0;
  }

  $csv->set ( filter => \&filter, max_items => scalar @PARS);
  $csv->load_csv_file_body (*FI);
  close (FI);

  print "csv: ", Dumper ($csv);

  # TODO: order data by fo_count and fo_pos_beg!

  my $cnt_items= 0;
  foreach my $row (@{$csv->{'data'}})
  {
    load_item ($row); # TODO: check for errors etc.
    $cnt_items++;
  }

  return $cnt_items;
}

sub get_items
{
  my $csv= shift;   # NOTE: the csv file is not used in this mode!
  my $pars= shift;

  unless (defined ($pars) && @$pars)
  {
    print "no items specified\n";
    return undef;
  }

  $pds->{do_read}= 1;

  my @rec_nums=();
  foreach my $item (@$pars)
  {
    if ($item =~ m#^Q(\d+)$#)
    {
      push (@rec_nums, $1);
    }
  }

  my $cnt_items= 0;
  foreach my $rec_num (sort { $a <=> $b } @rec_nums)
  {
    my $data= $pds->retrieve ($rec_num);
    # main::hexdump ($data);
    my ($x_rec_num, $pos_idx, $f_num, $beg, $end, @x)= unpack ('LLLLLLLL', $data);

    # recreate most importent parts of one row from items.csv 
    my $row=
    {
      id         => 'Q'.$x_rec_num,
      fo_count   => $f_num,
      fo_pos_beg => $beg,
      fo_pos_end => $end,
    };

    if ($x_rec_num > 0)
    {
      load_item ($row); # TODO: check for errors etc.
      $cnt_items++;
    }
    else
    {
      print "item not found in index\n";
      print "rec_num=[$rec_num] x_rec_num=[$x_rec_num] pos_idx=[$pos_idx] f_num=[$f_num] beg=[$beg] end=[$end]\n";
    }

  }


  return $cnt_items;
}

sub usage
{
  system ('perldoc', $0);
  exit;
}

sub load_item
{
  my $row= shift;

  print "row: ", Dumper ($row);
  
  my ($id, $f_num, $beg, $end)= map { $row->{$_} } qw(id fo_count fo_pos_beg fo_pos_end);
  my $size= $end-$beg;
  my $fnm_data= sprintf ('%s/wdq%05d.cmp', $out_dir, $row->{'fo_count'});

  print "id=[$id] f_num=[$f_num] fnm_data=[$fnm_data] beg=[$beg] end=[$end] size=[$size]\n";

  open (FD, '<:raw', $fnm_data);
  seek (FD, $beg, 0);
  my $buffer;
  sysread (FD, $buffer, $size);
  my $json= uncompress ($buffer);
  # print "json: ", Dumper ($json);
  my $data= JSON::decode_json ($json);
  print "data: ", Dumper ($data);

  $data;
}

package PDS;

sub new
{
  my $class= shift;

  my $self= {};
  bless $self, $class;
  $self->set(@_);

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
  foreach my $an (keys %defaults)
  {
    $self->{$an}= $defaults{$an} unless (exists ($self->{$an}));
  }

  # derived semi-constants
  $self->{recs_per_page}= int (($self->{page_size} - $self->{page_hdr_size}) / $self->{rec_size});
  # print "recs_per_page=[$recs_per_page]\n"; exit;

  local *FPDS;
  unless (open (FPDS, '+<:raw', $self->{backing_file}))
  {
    die "can not create paging backing file [$self->{backing_file}]";
  }
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
  print "pdsp: rec_num=[$rec_num] page_num=[$pdsp->{page_num}] rel_rec_num=[$rel_rec_num] rel_rec_pos=[$rel_rec_pos]\n";
  my $d= substr ($pdsp->{buffer}, $rel_rec_pos, $self->{rec_size});
  print "d:\n";
  main::hexdump ($d);
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

