
package Wiktionary::Text;

use Data::Dumper;

sub analyze_wiki_text
{
  my $lines= shift;

  my $type= 'unknown';
  my $language= 'unknown';

  return ('empty', $language, []) unless (@$lines);

  # print __LINE__, " analyze_wiki_text: ", Dumper($lines);

  my @errors;

  TEXT: foreach my $l (@$lines)
  {
    print __LINE__, " [$l]\n";

    if ($l =~ m#^=#)
    {
      my @tokens= split(' ', $l);

      my $hl_o= shift(@tokens);
      my $hl_c= pop(@tokens);

      if ($hl_o ne $hl_c)
      {
        push (@errors, ['heading mismatch', $l, "hl_o=[$hl_o] hl_c=[$hl_c]", \@tokens]);
        next TEXT;
      }

      my $hl= length($hl_o);
      print __LINE__, " heading level=[$hl] tokens: ", Dumper(\@tokens);

      my ($words, $macro_infos)= analyze_heading_tokens(@tokens);
      print __LINE__, " words: ", Dumper($words);
      print __LINE__, " macro_infos: ", Dumper($macro_infos);
    }
  }

  if (@errors)
  {
    print __LINE__, " errors: ", Dumper(\@errors);
  }

  return ($type, $language, \@errors);
}

sub analyze_heading_tokens
{
  my @tokens= @_;

  my @words= ();
  my @macro_infos= ();

      while (my $token= shift(@tokens))
      {
        if ($token=~ m#^\(?\{\{(.+)}}\)?#)
        {
          my $macro= $1;
          push (@macro_infos, process_macro($macro));
        }
        elsif ($token =~ m#^\(?\{\{(.+)#)
        {
          my $macro= $1;

          T2: while (my $t2= shift(@tokens)) # find the end of the macro
          {
            if ($t2 =~ m#(.+)}}\)?,?$#)  # there could be several macros, separated by ,
            {
              $macro .= ' ' . $1;
              last T2;
            }
            else
            {
              $macro .= ' '. $t2;
            }
          }

          print __LINE__, " macro=[$macro]\n";

          push (@macro_infos, process_macro($macro));
        }
        else
        {
          push (@words, $token);
        }
      }

  print __LINE__, " words: ", Dumper(\@words);
  print __LINE__, " macro_infos: ", Dumper(\@macro_infos);

  (\@words, \@macro_infos);
}

sub process_macro
{
  my $macro_string= shift;

  my @elements= split (/\|/, $macro_string);
  print __LINE__, " elements: ", Dumper(\@elements);

  \@elements;
}


1;

__END__

=head1 NOTES

=head2 heading level 2

 format: == string ({{language_label|language}}) ==

there can be several sections for the same title representing several languages


