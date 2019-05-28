/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.directives.currency;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.Row;
import org.apache.commons.lang3.LocaleUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Tests {@link ParseAsCurrency}
 */
public class FormatAsCurrencyTest {

  @Test
  public void testDefaultOption() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("src", "$1.56"),
      new Row("src", "$45.56"),
      new Row("src", "$6.78"),
      new Row("src", "$0.09"),
      new Row("src", "1234.56"),
      new Row("src", "$8,976.78"),
      new Row("src", "$58,976.78"),
      new Row("src", "$1,234,678.67")
    );

    String[] directives = new String[] {
      "parse-as-currency :src :dst",
      "parse-as-currency :src :dst1 'en_US'",
      "format-as-currency :dst :fmt1 'en_US'"
    };

    double[] expected = new double[] {
      1.56,
      45.56,
      6.78,
      0.09,
      8976.78,
      58976.78,
      1234678.67
    };

    Pair<List<Row>, List<Row>> result = TestingRig.executeWithErrors(directives, rows);
    List<Row> results = result.getFirst();
    List<Row> errors = result.getSecond();

    Assert.assertEquals(7, results.size());
    Assert.assertEquals(1, errors.size());

    int i = 0;
    for (Row row : results) {
      double val = (double) row.getValue("dst");
      Assert.assertEquals(expected[i], val, 0.001);
      ++i;
    }

    i = 0;
    for (Row row : results) {
      double val = (double) row.getValue("dst1");
      Assert.assertEquals(expected[i], val, 0.001);
      ++i;
    }

    for (Row row : results) {
      String src = (String) row.getValue("src");
      String dst = (String) row.getValue("fmt1");
      Assert.assertEquals(src, dst);
    }
  }

  @Test
  public void testUSDToEUR() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("src", 1.56),
      new Row("src", 45.56),
      new Row("src", 6.78),
      new Row("src", 0.09),
      new Row("src", 1234.56),
      new Row("src", 8976.78),
      new Row("src", 58976.78),
      new Row("src", 1234678.67)
    );

    String[] directives = new String[] {
      "format-as-currency :src :dst 'en_IE'"
    };

    Pair<List<Row>, List<Row>> result = TestingRig.executeWithErrors(directives, rows);
    List<Row> results = result.getFirst();
    List<Row> errors = result.getSecond();

    Assert.assertEquals(8, results.size());
    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void testGetLocale() throws Exception {
    List<Locale> locales = LocaleUtils.availableLocaleList();
    Assert.assertTrue(locales.size() > 0);
  }
}
