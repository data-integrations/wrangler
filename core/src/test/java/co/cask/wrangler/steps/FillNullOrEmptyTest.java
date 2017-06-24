/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.transformation.FillNullOrEmpty;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link FillNullOrEmpty}
 */
public class FillNullOrEmptyTest {

  @Test
  public void testColumnNotPresent() throws Exception {
    String[] directives = new String[] {
      "fill-null-or-empty null N/A",
    };

    List<Record> records = Arrays.asList(
      new Record("value", "has value"),
      new Record("value", null),
      new Record("value", null)
    );

    RecipePipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 3);
  }

  @Test
  public void testBasicNullCase() throws Exception {
    String[] directives = new String[] {
      "fill-null-or-empty value N/A",
    };

    List<Record> records = Arrays.asList(
      new Record("value", "has value"),
      new Record("value", null),
      new Record("value", null)
    );

    records = RecipePipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals("has value", records.get(0).getValue("value"));
    Assert.assertEquals("N/A", records.get(1).getValue("value"));
    Assert.assertEquals("N/A", records.get(2).getValue("value"));
  }

  @Test
  public void testEmptyStringCase() throws Exception {
    String[] directives = new String[] {
      "fill-null-or-empty value N/A",
    };

    List<Record> records = Arrays.asList(
      new Record("value", ""),
      new Record("value", ""),
      new Record("value", "Should be fine")
    );

    records = RecipePipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals("N/A", records.get(0).getValue("value"));
    Assert.assertEquals("N/A", records.get(1).getValue("value"));
    Assert.assertEquals("Should be fine", records.get(2).getValue("value"));
  }

  @Test
  public void testMixedCases() throws Exception {
    String[] directives = new String[] {
      "fill-null-or-empty value N/A",
    };

    List<Record> records = Arrays.asList(
      new Record("value", null),
      new Record("value", ""),
      new Record("value", "Should be fine")
    );

    records = RecipePipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals("N/A", records.get(0).getValue("value"));
    Assert.assertEquals("N/A", records.get(1).getValue("value"));
    Assert.assertEquals("Should be fine", records.get(2).getValue("value"));
  }
}