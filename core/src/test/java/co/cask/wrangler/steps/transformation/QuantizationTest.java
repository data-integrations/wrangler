/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Quantization}
 */
public class QuantizationTest {
  @Test
  public void testQuanitization() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "quantize hrlywage wagerange 0.0:20.0=LOW,21.0:75.0=MEDIUM,75.1:200.0=HIGH",
      "set column wagerange (wagerange == null) ? \"NOT FOUND\" : wagerange"
    };

    List<Record> records = Arrays.asList(
      new Record("__col", "1098,Root,Joltie,01/26/1956,root@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1091,Root,Joltie,01/26/1956,root1@joltie.io,32,129.13,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1092,Root,Joltie,01/26/1956,root@mars.com,32,9.54,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1093,Root,Joltie,01/26/1956,root@foo.com,32,7.89,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1094,Root,Joltie,01/26/1956,windy@joltie.io,32,45.67,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1094,Root,Joltie,01/26/1956,windy@joltie.io,32,20.7,150 Mars Ave,Palo Alto,CA,USA,32826")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 6);
    int low = 0, medium = 0, high = 0, notfound = 0;
    for (Record record : records) {
      String v = (String) record.getValue("wagerange");
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
}
