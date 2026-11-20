/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package io.cdap.wrangler.dq;

 import org.junit.Assert;
 import org.junit.Test;
 
 /**
  * Unit tests for ConvertString functions.
  */
 public class ConvertStringTest {
   private static final String expected = "abc";
 
   @Test
   public void testRemoveTrailingAndLeading() {
     ConvertString convertString = new ConvertString();
 
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading(expected));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading(" abc"));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading(" abc "));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading(" abc  "));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading("  abc "));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading("  abc  "));
     Assert.assertEquals("ab c", convertString.removeTrailingAndLeading(" ab c"));
     Assert.assertEquals("a b c", convertString.removeTrailingAndLeading(" a b c "));
 
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading("\t" + expected, "\t"));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading(expected + "\t", "\t"));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading('\u0009' + expected, "\t"));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading('\u0009' + expected, '\u0009' + ""));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading('\u0009' + expected + '\u0009' + '\u0009', "\t"));
     Assert.assertEquals("abc ", convertString.removeTrailingAndLeading("\t" + "abc ", "\t"));
     Assert.assertEquals("a\tbc", convertString.removeTrailingAndLeading("\t" + "a" + "\t" + "bc", "\t"));
     Assert.assertEquals("\tabc", convertString.removeTrailingAndLeading("\t" + "abc "));
     Assert.assertEquals(expected, ("\t" + "abc ").trim());
 
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading("\n" + expected, "\n"));
     Assert.assertEquals("abc ", convertString.removeTrailingAndLeading("\n" + "abc ", "\n"));
 
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading(expected, "\r"));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading("\r" + expected, "\r"));
     Assert.assertEquals(expected, convertString.removeTrailingAndLeading("\r" + expected + "\r", "\r"));
     Assert.assertEquals("abc ", convertString.removeTrailingAndLeading("\r" + "abc ", "\r"));
     Assert.assertEquals("abc ", convertString.removeTrailingAndLeading("\r" + "abc " + "\r", "\r"));
 
     Assert.assertEquals("bc", convertString.removeTrailingAndLeading(" abc", " a"));
     Assert.assertEquals(" a", convertString.removeTrailingAndLeading(" abc", "bc"));
     Assert.assertEquals("ab", convertString.removeTrailingAndLeading("cabc", "c"));
   }
 
   @Test
   public void testRemoveTrailingAndLeadingWhitespaces() {
     ConvertString convertString = new ConvertString();
     String inputData = " " + expected;
     for (String removeChar : convertString.WHITESPACE_CHARS) {
       inputData = inputData + removeChar;
     }
     Assert.assertEquals(expected, convertString.removeTrailingAndLeadingWhitespaces(inputData));
   }
 
   @Test
   public void testRemoveDuplicate_CR() {
     ConvertString convertString = new ConvertString("\r");
     String input = "a\rbccccdeaa\r\r\ry";
     Assert.assertEquals("a\rbccccdeaa\ry", convertString.removeRepeatedChar(input));
   }
 
   @Test
   public void testRemoveDuplicate_LF() {
     ConvertString convertString = new ConvertString("\n");
     String input = "a\nbccccdeaa\n\n\ny";
     Assert.assertEquals("a\nbccccdeaa\ny", convertString.removeRepeatedChar(input));
   }
 
   @Test
   public void testRemoveDuplicate_CRLF() {
     ConvertString convertString = new ConvertString("\r\n");
     String input = "a\r\nbccccdeaa\r\n\r\n\r\ny";
     Assert.assertEquals("a\r\nbccccdeaa\r\ny", convertString.removeRepeatedChar(input));
   }
 
   @Test
   public void testRemoveDuplicate_TAB() {
     ConvertString convertString = new ConvertString("\t");
     String input = "a\tbccccdeaa\t\t\t\t\t\ty";
     Assert.assertEquals("a\tbccccdeaa\ty", convertString.removeRepeatedChar(input));
   }
 
   @Test
   public void testRemoveDuplicate_LETTER() {
     ConvertString convertString = new ConvertString("c");
     String input = "atbccccdeaaCCtcy";
     Assert.assertEquals("atbcdeaaCCtcy", convertString.removeRepeatedChar(input));
 
     convertString = new ConvertString("a");
     input = "aaatbccccdeaaCCtcy";
     Assert.assertEquals("atbccccdeaCCtcy", convertString.removeRepeatedChar(input));
 
     convertString = new ConvertString("ac");
     input = "acacacactbccccdeaCCtaccy";
     Assert.assertEquals("actbccccdeaCCtaccy", convertString.removeRepeatedChar(input));
 
     input = "abcdef";
     Assert.assertEquals("abcdef", convertString.removeRepeatedChar(input));
   }
 
   @Test
   public void testRemoveDuplicate_NULL1() {
     ConvertString convertString = new ConvertString("c");
     String input = null;
     Assert.assertNull(convertString.removeRepeatedChar(input));
     input = "";
     Assert.assertEquals("", convertString.removeRepeatedChar(input));
   }
 
   @Test
   public void testRemoveDuplicate_NULL2() {
     ConvertString convertString = new ConvertString();
     String input = "aaabc";
     Assert.assertEquals(input, convertString.removeRepeatedChar(input));
 
     convertString = new ConvertString("");
     Assert.assertEquals(input, convertString.removeRepeatedChar(input));
 
     convertString = new ConvertString(null);
     Assert.assertEquals(input, convertString.removeRepeatedChar(input));
   }
 
   @Test
   public void testremoveWhiteSpace() {
     ConvertString convertString = new ConvertString();
 
     String input = "a   b\t\t\tc\n\n\nd\r\re\f\ff";
     String cleanStr = convertString.removeRepeatedWhitespaces(input);
     Assert.assertEquals("a b\tc\nd\re\ff", cleanStr);
 
     input = "aaab\r\n\r\n\r\nx";
     cleanStr = convertString.removeRepeatedWhitespaces(input);
     Assert.assertEquals("aaab\r\n\r\n\r\nx", cleanStr);
 
     input = "a\u0085\u0085\u0085b\u00A0\u00A0c\u1680\u1680d\u180E\u180Ee\u2000\u2000f\u2001\u2001g"
            + "\u2002\u2002h\u2003\u2003i\u2004\u2004";
     cleanStr = convertString.removeRepeatedWhitespaces(input);
     Assert.assertEquals("a\u0085b\u00A0c\u1680d\u180Ee\u2000f\u2001g\u2002h\u2003i\u2004", cleanStr);
 
     input = "a\u2005\u2005\u2005b\u2006\u2006c\u2007\u2007d\u2008\u2008e\u2009\u2009f\u200A\u200Ag"
            + "\u2028\u2028h\u2029\u2029i\u202F\u202Fj\u205F\u205Fk\u3000\u3000l";
     cleanStr = convertString.removeRepeatedWhitespaces(input);
     String expectedCleaned = "a\u2005b\u2006c\u2007d\u2008e\u2009f\u200Ag\u2028h\u2029i\u202Fj\u205Fk\u3000l";
     Assert.assertEquals(expectedCleaned, cleanStr);
   }
 
   @Test
   public void testRemoveWhiteSpaceNull() {
     ConvertString convertString = new ConvertString();
     String input = "";
     String cleanStr = convertString.removeRepeatedWhitespaces(input);
     Assert.assertEquals("", cleanStr);
     input = null;
     cleanStr = convertString.removeRepeatedWhitespaces(input);
     Assert.assertNull(cleanStr);
   }
 
   @Test
   public void testRemoveWhiteSpacWithoutSpace() {
     ConvertString convertString = new ConvertString();
     String input = "abccdef";
     String cleanStr = convertString.removeRepeatedWhitespaces(input);
     Assert.assertEquals("abccdef", cleanStr);
   }
 }
 