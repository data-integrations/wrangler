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

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests {@link Quantization}
 */
public class QuantizationTest {
  @Test
  public void testQuanitization() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "quantize hrlywage wagerange 0.0:20.0='LOW',21.0:75.0='MEDIUM',75.1:200.0='HIGH'",
      "set column wagerange (wagerange == null) ? \"NOT FOUND\" : wagerange"
    };

    List<Row> rows = Arrays.asList(
      new Row("__col", "1098,Root,Joltie,01/26/1956,root@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1091,Root,Joltie,01/26/1956,root1@joltie.io,32,129.13,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1092,Root,Joltie,01/26/1956,root@mars.com,32,9.54,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1093,Root,Joltie,01/26/1956,root@foo.com,32,7.89,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1094,Root,Joltie,01/26/1956,windy@joltie.io,32,45.67,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1094,Root,Joltie,01/26/1956,windy@joltie.io,32,20.7,150 Mars Ave,Palo Alto,CA,USA,32826")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 6);
    int low = 0, medium = 0, high = 0, notfound = 0;
    for (Row row : rows) {
      String v = (String) row.getValue("wagerange");
      if (v.equalsIgnoreCase("NOT FOUND")) {
        notfound++;
      } else if (v.equals("LOW")) {
        low++;
      } else if (v.equals("MEDIUM")) {
        medium++;
      } else if (v.equals("HIGH")) {
        high++;
      }
    }

    Assert.assertEquals(3, low);
    Assert.assertEquals(1, medium);
    Assert.assertEquals(1, high);
    Assert.assertEquals(1, notfound);
  }

  @Test
  public void testQuantizationRangeAndPattern() throws Exception {
    RangeMap<Double, String> rangeMap = TreeRangeMap.create();
    rangeMap.put(Range.closed(0.1, 0.9), "A");
    rangeMap.put(Range.closed(2.0, 3.9), "B");
    rangeMap.put(Range.closed(4.0, 5.9), "C");
    String s = rangeMap.get(2.2);
    Assert.assertEquals("B", s);

    Matcher m = Pattern.compile("([+-]?\\d+(?:\\.\\d+)?):([+-]?\\d+(?:\\.\\d+)?)=(.[^,]*)")
      .matcher("0.9:2.1=Foo,2.2:3.4=9.2");
    RangeMap<String, String> rm = TreeRangeMap.create();
    while (m.find()) {
      String lower = m.group(1);
      String upper = m.group(2);
      String value = m.group(3);
      rm.put(Range.closed(lower, upper), value);
    }
    Assert.assertEquals("[[0.9..2.1]=Foo, [2.2..3.4]=9.2]", rm.toString());
  }
}
