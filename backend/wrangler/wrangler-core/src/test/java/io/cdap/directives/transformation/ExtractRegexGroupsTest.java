/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.transformation;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests {@link ExtractRegexGroups}
 */
public class ExtractRegexGroupsTest {

  static {
    String dateSeparatorRegex = "(?:(?:[.,]\\s)|[-/.\\s])";
    String dayRegex = "(?:\\d{1,2})";
    String yearRegex = "(?:\\d{4}|\\d{2})";
    String monthRegex = "(?:(?:1[0-2])|" +
      "(?:0?\\d)|" +
      "(?:[a-zA-Z]{3}))";
    DATE_REGEX = String.format("(?:%s%s%s%s%s)|(?:(?:%s%s%s|%s%s%s)%s%s)",
                                 yearRegex, dateSeparatorRegex, monthRegex, dateSeparatorRegex, dayRegex,
                                 dayRegex, dateSeparatorRegex, monthRegex,
                                 monthRegex, dateSeparatorRegex, dayRegex,
                                 dateSeparatorRegex, yearRegex);

    String hoursMinutes = "(?:(?:2[0-3])|(?:[01]?\\d))[h:\\s][0-5]\\d";
    String seconds = "(?::(?:(?:[0-5]\\d)|(?:60)))?";
    String ampm = "(?:\\s[aApP][mM])?";
    String timezone = "(?:Z|(?:[+-](?:1[0-2])|(?:0?\\d):[0-5]\\d)|(?:\\s[[a-zA-Z]\\s]+))?";
    TIME_REGEX = hoursMinutes + seconds + ampm + timezone;
  }

  public static final String DATE_REGEX;
  public static final String TIME_REGEX;

  private static class RegexInputOutput {
    public final String input;
    public final String[] output;

    public RegexInputOutput(String input, String... output) {
      this.input = input;
      this.output = output;
    }
  }

  @Test
  public void testRegexGroups() throws Exception {
    String regex = "[^(]+\\(([0-9]{4})\\).*";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("Toy Story (1995)", "1995"),
      new RegexInputOutput("Toy Story")
    };

    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testEmailRegex() throws Exception {
    String regex = "((?:\\S+|\".*?\")+@[a-zA-Z0-9\\.-]+(?:\\.[a-zA-Z]{2,6})?)";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("this is not an email"),
      new RegexInputOutput("@"),
      new RegexInputOutput("test@example.com", "test@example.com"),
      new RegexInputOutput("two@emails.com, another@one.com", "two@emails.com", "another@one.com"),
      new RegexInputOutput("this.email.is.dotted@dots.com", "this.email.is.dotted@dots.com"),
      new RegexInputOutput("this_underscore@email.com", "this_underscore@email.com"),
      new RegexInputOutput("gmail+has@plusses.com", "gmail+has@plusses.com"),
      new RegexInputOutput("mixed_email.stuff+thing@example.com", "mixed_email.stuff+thing@example.com"),
      new RegexInputOutput("MiXeD@case.com", "MiXeD@case.com"),
      new RegexInputOutput("thishasn0mbers@example.com", "thishasn0mbers@example.com"),
      new RegexInputOutput("tldemail@tld", "tldemail@tld")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testPhoneNumber() throws Exception {
    String regex = "((?:\\+\\d{1,3}[\\s-]?)?\\(?\\d{3}\\)?[\\s-]?\\d{3}[\\s-]?\\d{4})";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("+12345678901", "+12345678901"),
      new RegexInputOutput("+1-234-567-8901", "+1-234-567-8901"),
      new RegexInputOutput("+1-(234)-567-8901", "+1-(234)-567-8901"),
      new RegexInputOutput("+1 (234)-567-8901", "+1 (234)-567-8901"),
      new RegexInputOutput("+1 (234) 567-8901", "+1 (234) 567-8901"),
      new RegexInputOutput("+1 (234) 567 8901", "+1 (234) 567 8901"),
      new RegexInputOutput("+1 234 567 8901", "+1 234 567 8901"),
      new RegexInputOutput("(123)-456-7890", "(123)-456-7890"),
      new RegexInputOutput("(123) 456-7890", "(123) 456-7890"),
      new RegexInputOutput("(123) 456 7890", "(123) 456 7890"),
      new RegexInputOutput("123 456 7890", "123 456 7890"),
      new RegexInputOutput("1234567890", "1234567890")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testSSN() throws Exception {
    String regex = "(\\d{3}[-\\s]?\\d{2}[-\\s]?\\d{4})";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("not an ssn"),
      new RegexInputOutput("123-45-6789", "123-45-6789"),
      new RegexInputOutput("123 45 6789", "123 45 6789"),
      new RegexInputOutput("123456789", "123456789"),
      new RegexInputOutput("1234578"),
      new RegexInputOutput("123-45-78"),
      new RegexInputOutput("123456789 1234578 123 45 6789 abc 123-45-6789 1234-56-789",
                           "123456789", "123 45 6789", "123-45-6789")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testCreditCard() throws Exception {
    String regex = "((?:\\d{4}[-\\s]?){4})";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("1234567890123456", "1234567890123456"),
      new RegexInputOutput("1234-5678-9012-3456", "1234-5678-9012-3456"),
      new RegexInputOutput("1234 5678 9012 3456", "1234 5678 9012 3456"),
      new RegexInputOutput("1234 5678 9012 3456, 1234-5678-9012-3456", "1234 5678 9012 3456", "1234-5678-9012-3456"),
      new RegexInputOutput("123456789012345"),
      new RegexInputOutput("1234-5678-9012-345"),
      new RegexInputOutput("123-45678-9012-3456"),
      new RegexInputOutput("this is not a credit card number")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testURL() throws Exception {
    String regex = "((?:https?://)?[a-zA-Z0-9\\.-]+\\.[a-zA-Z]{2,6}(?:/[\\w\\.-]+)*(?:\\?[\\w\\.&=\\-]+)?)";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("cask.co", "cask.co"),
      new RegexInputOutput("http://cask.co", "http://cask.co"),
      new RegexInputOutput("https://cask.co", "https://cask.co"),
      new RegexInputOutput("https://cask.co/test", "https://cask.co/test"),
      new RegexInputOutput("https://cask.co/test/anotherone", "https://cask.co/test/anotherone"),
      new RegexInputOutput("https://cask.co/test/anotherone?query=1&other=2",
                           "https://cask.co/test/anotherone?query=1&other=2"),
      new RegexInputOutput("this is not a website")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testIPAddress() throws Exception {
    String regex = "((?:(?:0|(?:25[0-5])|(?:2[0-4][1-9])|(?:1\\d\\d)|(?:[1-9]\\d?))\\.){3}" +
      "(?:(?:0|(?:25[0-5])|(?:2[0-4][1-9])|(?:1\\d\\d)|(?:[1-9]\\d?))))";

    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("192.168.1.1", "192.168.1.1"),
      new RegexInputOutput("0.0.0.0", "0.0.0.0"),
      new RegexInputOutput("255.255.255.0", "255.255.255.0"),
      new RegexInputOutput("12.34.123.0", "12.34.123.0"),
      new RegexInputOutput("255.0.0.0", "255.0.0.0"),
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testMacAddress() throws Exception {
    String regex = "((?:\\p{XDigit}{2}[:-]){5}(?:\\p{XDigit}{2}))";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("ab:cd:ef:12:34:56", "ab:cd:ef:12:34:56"),
      new RegexInputOutput("ab-cd-ef-12-34-56", "ab-cd-ef-12-34-56"),
      new RegexInputOutput("abcdef123456"),
      new RegexInputOutput("ab:cd:ef:12:34:5g"),
      new RegexInputOutput("ab-cd-ef-12-34-5g")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testHTMLTag() throws Exception {
    String regex = "<([a-zA-Z]+)(?:\\s+[a-zA-Z]+=\".*?\")*(?:(?:>(.*)</\\1>)|(?:\\s*/?>))";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("<selfclose>", "selfclose", null),
      new RegexInputOutput("<selfclose />", "selfclose", null),
      new RegexInputOutput("<selfclose/>", "selfclose", null),
      new RegexInputOutput("<tag>content</tag>", "tag", "content"),
      new RegexInputOutput("<tag color=\"orange\">content</tag>", "tag", "content")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testLinkTag() throws Exception {
    String regex = "<[aA](?:\\s+[a-zA-Z]+=\".*?\")*\\s+[hH][rR][eE][fF]=\"(.*?)\"(?:\\s+[a-zA-Z]+=\".*?\")*>" +
      "(.*)" +
      "</[aA]>";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("<a href=\"cask.co\">Cask Data</a>", "cask.co", "Cask Data"),
      new RegexInputOutput("<a target=\"_self\" href=\"http://cask.co/\">Cask Data</a>", "http://cask.co/",
                           "Cask Data"),
      new RegexInputOutput("<a  target=\"_self\"    href=\"http://cask.co/\">Cask Data</a>",
                           "http://cask.co/", "Cask Data"),
      new RegexInputOutput("<a  target=\"_self\"    href=\"http://cask.co/\" type=\"text\">Cask Data</a>",
                           "http://cask.co/", "Cask Data")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testDateRegex() throws Exception {
    String regex = String.format("(%s)", DATE_REGEX);
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("2012 Dec 21", "2012 Dec 21"),
      new RegexInputOutput("12/25/2017", "12/25/2017"),
      new RegexInputOutput("Dec/25/2017", "Dec/25/2017"),
      new RegexInputOutput("Dec 25, 2017", "Dec 25, 2017"),
      new RegexInputOutput("1997-10-16", "1997-10-16")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testTimeRegex() throws Exception {
    String regex = String.format("(%s)", TIME_REGEX);
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("23:59", "23:59"),
      new RegexInputOutput("12:59 PM", "12:59 PM"),
      new RegexInputOutput("12:59:59 AM", "12:59:59 AM"),
      new RegexInputOutput("12:59:60 PM", "12:59:60 PM"),
      new RegexInputOutput("12h59", "12h59"),
      new RegexInputOutput("12:59:04Z", "12:59:04Z"),
      new RegexInputOutput("02:59:04Z", "02:59:04Z"),
      new RegexInputOutput("2:59:04Z", "2:59:04Z"),
      new RegexInputOutput("2:59:04 PST", "2:59:04 PST"),
      new RegexInputOutput("2:59:04 Pacific Standard Time", "2:59:04 Pacific Standard Time")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testDateTimeRegex() throws Exception {
    String regex = String.format("((?:%s)[T\\s](?:%s))", DATE_REGEX, TIME_REGEX);
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("Dec 1, 2015 23:59", "Dec 1, 2015 23:59"),
      new RegexInputOutput("JUL/05/2010 12:59 PM", "JUL/05/2010 12:59 PM"),
      new RegexInputOutput("10/16/97 12:59:59 AM", "10/16/97 12:59:59 AM"),
      new RegexInputOutput("10-10-10 12:59:60 PM", "10-10-10 12:59:60 PM"),
      new RegexInputOutput("Dec 31, 1997 12h59", "Dec 31, 1997 12h59"),
      new RegexInputOutput("2018-03-04T12:59:04Z", "2018-03-04T12:59:04Z"),
      new RegexInputOutput("2016-01-10T02:59:04Z", "2016-01-10T02:59:04Z"),
      new RegexInputOutput("2016-01-10 2:59:04Z", "2016-01-10 2:59:04Z"),
      new RegexInputOutput("2016-01-10 2:59:04 PST", "2016-01-10 2:59:04 PST"),
      new RegexInputOutput("2016-01-10 2:59:04 Pacific Standard Time", "2016-01-10 2:59:04 Pacific Standard Time")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testUPSRegex() throws Exception {
    String regex = "(1Z\\s?[0-9a-zA-Z]{3}\\s?[0-9a-zA-Z]{3}\\s?[0-9a-zA-Z]{2}\\s?\\d{4}\\s?\\d{4})";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("1Z204E380338943508", "1Z204E380338943508"),
      new RegexInputOutput("1Z51062E6893884735", "1Z51062E6893884735"),
      new RegexInputOutput("1ZXF38300382722839", "1ZXF38300382722839"),
      new RegexInputOutput("1ZT675T4YW92275898", "1ZT675T4YW92275898"),
      new RegexInputOutput("1ZW6897XYW00098770", "1ZW6897XYW00098770"),
      new RegexInputOutput("1Z 999 AA1 01 2345 6784", "1Z 999 AA1 01 2345 6784")
    };
    testRegex(regex, regexInputOutputs);
  }

  @Test
  public void testISBNRegex() throws Exception {
    String regex = "((?:97[89]-?)?(?:\\d-?){9}[\\dxX])";
    RegexInputOutput[] regexInputOutputs = {
      new RegexInputOutput("ISBN-13: 978-1-56619-909-4", "978-1-56619-909-4"),
      new RegexInputOutput("ISBN-10: 1-56619-909-3", "1-56619-909-3"),
      new RegexInputOutput("ISBN: 9781566199094", "9781566199094"),
      new RegexInputOutput("ISBN-10: 1566199093", "1566199093"),
      new RegexInputOutput("ISBN: 978156619909x", "978156619909x"),
      new RegexInputOutput("ISBN-10: 156619909X", "156619909X"),
      new RegexInputOutput("ISBN 817525766-0", "817525766-0"),
      new RegexInputOutput("ISBN 0-936385-405", "0-936385-405")
    };
    testRegex(regex, regexInputOutputs);
  }

  private void testRegex(String regex, RegexInputOutput[] regexInputOutputs) throws Exception {
    final String column = "column";

    String[] directives = new String[] {
      String.format("extract-regex-groups %s %s", column, regex)
    };

    List<Row> rows = new LinkedList<>();
    for (RegexInputOutput regexInputOutput : regexInputOutputs) {
      rows.add(new Row(column, regexInputOutput.input));
    }

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(regexInputOutputs.length, rows.size());
    for (int i = 0; i < regexInputOutputs.length; i++) {
      RegexInputOutput regexInputOutput = regexInputOutputs[i];

      Assert.assertEquals(regexInputOutput.input, regexInputOutput.output.length, rows.get(i).width() - 1);
      for (int j = 0; j < regexInputOutput.output.length; j++) {
        Assert.assertEquals(regexInputOutput.output[j], rows.get(i).getValue(j + 1));
      }
    }
  }
}
