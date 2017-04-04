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
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.steps.column.Copy;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Copy}
 */
public class CopyTest {

  @Test
  public void testBasicCopy() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "copy body_1 name"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "A,B,1"),
      new Record("body", "D,E,2"),
      new Record("body", "G,H,3")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals(5, records.get(0).length()); // should have copied to another column
    Assert.assertEquals("A", records.get(0).getValue("name")); // Should have copy of 'A'
    Assert.assertEquals("D", records.get(1).getValue("name")); // Should have copy of 'D'
    Assert.assertEquals("G", records.get(2).getValue("name")); // Should have copy of 'G'
    Assert.assertEquals(records.get(0).getValue("name"), records.get(0).getValue("body_1"));
    Assert.assertEquals(records.get(1).getValue("name"), records.get(1).getValue("body_1"));
    Assert.assertEquals(records.get(2).getValue("name"), records.get(2).getValue("body_1"));
  }

  @Test(expected = StepException.class)
  public void testCopyToExistingColumn() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "copy body_1 body_2"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "A,B,1"),
      new Record("body", "D,E,2"),
      new Record("body", "G,H,3")
    );

    records = PipelineTest.execute(directives, records);
  }

  @Test
  public void testForceCopy() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "copy body_1 body_2 true"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "A,B,1"),
      new Record("body", "D,E,2"),
      new Record("body", "G,H,3")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals(4, records.get(0).length()); // should have copied to another column
    Assert.assertEquals("A", records.get(0).getValue("body_2")); // Should have copy of 'A'
    Assert.assertEquals("D", records.get(1).getValue("body_2")); // Should have copy of 'D'
    Assert.assertEquals("G", records.get(2).getValue("body_2")); // Should have copy of 'G'
    Assert.assertEquals(records.get(0).getValue("body_2"), records.get(0).getValue("body_1"));
    Assert.assertEquals(records.get(1).getValue("body_2"), records.get(1).getValue("body_1"));
    Assert.assertEquals(records.get(2).getValue("body_2"), records.get(2).getValue("body_1"));
  }

}