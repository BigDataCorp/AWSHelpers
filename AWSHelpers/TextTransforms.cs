/*
%     *
%COPYRIGHT* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *%
%                                                                          %
% AWS Class Helpers                                                        %
%                                                                          %
% Copyright (c) 2011-2014 Big Data Corporation ©                           %
%                                                                          %
%COPYRIGHT* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *%
      *
*/
using System;
using System.Linq;
using System.Net;
using System.Text;

namespace AWSHelpers
{
    public class TextTransforms
    {
        ///////////////////////////////////////////////////////////////////////
        //                           Fields                                  //
        ///////////////////////////////////////////////////////////////////////

        static readonly char[] ASCII_LOOKUP_TABLE_ACCENT =
        {
            'À', 'Á', 'Â', 'Ã', 'Ä', 'Å', 'Æ', 'Ç', 'È', 'É', 'Ê', 'Ë', 'Ì', 'Í', 'Î', 'Ï', 'Ð', 'Ñ',
            'Ò', 'Ó', 'Ô', 'Õ', 'Ö', '×', 'Ø', 'Ù', 'Ú', 'Û', 'Ü', 'Ý', 'Þ', 'ß', 'à', 'á', 'â',
            'ã', 'ä', 'å', 'æ', 'ç', 'è', 'é', 'ê', 'ë', 'ì', 'í', 'î', 'ï', 'ð', 'ñ', 'ò', 'ó',
            'ô', 'õ', 'ö', '÷', 'ø', 'ù', 'ú', 'û'
        };

        static readonly char[] ASCII_LOOKUP_TABLE_ACCENT_FREE =
        {
            'A', 'A', 'A', 'A', 'A', 'A', 'A', 'C', 'E', 'E', 'E', 'E', 'I', 'I', 'I', 'I', 'D', 'N',
            'O', 'O', 'O', 'O', 'O', 'X', '0', 'U', 'U', 'U', 'U', 'Y', 'Þ', 's', 'a', 'a', 'a',
            'a', 'a', 'a', 'a', 'c', 'e', 'e', 'e', 'e', 'i', 'i', 'i', 'i', 'o', 'n', 'o', 'o',
            'o', 'o', 'o', '÷', '0', 'u', 'u', 'u'
        };

        static readonly int ASCIILookupTableMinPos = ASCII_LOOKUP_TABLE_ACCENT[0];
        static readonly int ASCIILookupTableMaxPos = ASCII_LOOKUP_TABLE_ACCENT[ASCII_LOOKUP_TABLE_ACCENT.Length - 1];

        ///////////////////////////////////////////////////////////////////////
        //                    Methods & Functions                            //
        ///////////////////////////////////////////////////////////////////////

        public static char[] RemoveAccentuation(char[] newTxt)
        {
            for (int i = 0; i < newTxt.Length; ++i)
            {
                char c = newTxt[i];
                // update if in valid value range
                if ((c >= ASCIILookupTableMinPos) && (c <= ASCIILookupTableMaxPos))
                    newTxt[i] = ASCII_LOOKUP_TABLE_ACCENT_FREE[c - ASCIILookupTableMinPos];
            }
            return newTxt;
        }

        public static string RemoveAccentuation(string newTxt)
        {
            return new string(RemoveAccentuation(newTxt.ToCharArray()));
        }

        public static string HTMLTextDecodeAndUnescape(string value)
        {
            // Any text?
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            // Convert HTML special chars to their equivalent
            value = WebUtility.HtmlDecode(value);

            // Convert escaped string into their equivalent
            return Uri.UnescapeDataString(value);
        }

        public static string RemoveControls(string value)
        {
            StringBuilder sb = new StringBuilder(value.Length);
            char lastchar = 'a';
            foreach (char ch in value)
            {
                if (char.IsControl(ch))
                {
                    if (!Char.IsWhiteSpace(lastchar))
                    {
                        sb.Append(' ');
                        lastchar = ' ';
                    }
                }
                else
                {
                    if (!(Char.IsWhiteSpace(ch) && Char.IsWhiteSpace(lastchar)))
                    {
                        sb.Append(ch);
                        lastchar = ch;
                    }
                }
            }
            return sb.ToString();
        }

        public static string TextWithoutSpecials(string value)
        {
            // Any text?
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            // Convert HTML special chars to their equivalent and convert escaped string into their equivalent
            value = HTMLTextDecodeAndUnescape(value);

            // Remove controls
            value = RemoveControls(value);

            // Discard specials
            char oldchar = 'a';
            StringBuilder sb = new StringBuilder(value.Length);
            foreach (Char ch in value)
            {
                if (Char.IsWhiteSpace(ch) || Char.IsLetterOrDigit(ch) || ch.Equals('\'') || ch.Equals('-') || ch.Equals('.'))
                {
                    if (!(Char.IsWhiteSpace(ch) && Char.IsWhiteSpace(oldchar)))
                        sb.Append(ch);
                    oldchar = ch;
                }
                else
                {
                    if (!Char.IsWhiteSpace(oldchar))
                        sb.Append(' ');
                    oldchar = ' ';
                }
            }

            return sb.ToString();
        }

        public static string TextWithoutAccentuationAndSeparators(string src)
        {
            // Convert HTML special chars to their equivalent and convert escaped string into their equivalent
            src = HTMLTextDecodeAndUnescape(src);

            // Remove accentuation
            src = RemoveAccentuation(src);

            // Remove all special chars on the string. We replace the specials chars with blanks
            char lastChar = 'a';
            StringBuilder sb = new StringBuilder();
            foreach (char c in src)
            {
                if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == ' ')
                {
                    if (!((c.Equals(' ')) && (lastChar.Equals(' '))))
                        sb.Append(c);
                    lastChar = c;
                }
                else
                    if (!lastChar.Equals(' '))
                    {
                        sb.Append(' ');
                        lastChar = ' ';
                    }
            }
            return sb.ToString();
        }

        public static string TextWithWhiteSpaceOrLetterOrDigit(string value)
        {
            StringBuilder sb = new StringBuilder(value.Length);
            char oldchar = 'a';
            foreach (char ch in value)
            {
                if (Char.IsWhiteSpace(ch) || Char.IsLetterOrDigit(ch))
                {
                    if (!(Char.IsWhiteSpace(ch) && Char.IsWhiteSpace(oldchar)))
                        sb.Append(ch);
                    oldchar = ch;
                }
                else
                {
                    if (!Char.IsWhiteSpace(oldchar))
                        sb.Append(' ');
                    oldchar = ' ';
                }
            }
            return sb.ToString();
        }
    }
}
