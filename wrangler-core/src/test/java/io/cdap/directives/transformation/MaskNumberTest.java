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

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link MaskNumber}
 */
public class MaskNumberTest {

  @Test
  public void testSSNWithDashesExact() throws Exception {
    String[] directives = new String[] {
      "mask-number body xxx-xx-####"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "000-00-1234")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xxx-xx-1234", rows.get(0).getValue("body"));
  }

  @Test
  public void testSSNWithDashesExtra() throws Exception {
    String[] directives = new String[] {
      "mask-number body xxx-xx-#####"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "000-00-1234")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xxx-xx-1234", rows.get(0).getValue("body"));
  }

  @Test
  public void testComplexMasking() throws Exception {
    String[] directives = new String[] {
      "mask-number body xxx-##-xx-##-XXXX-9"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "0000012349898")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xxx-00-xx-34-xxxx-9", rows.get(0).getValue("body"));
  }

  @Test
  public void testIntegerTypeMasking() throws Exception {
    String[] directives = new String[] {
      "mask-number body xx-xx-#"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", 12345),
      new Row("body", 123),
      new Row("body", 123456)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(3, rows.size());
    Assert.assertEquals("xx-xx-5", rows.get(0).getValue("body"));
    Assert.assertEquals("xx-xx-", rows.get(1).getValue("body"));
    Assert.assertEquals("xx-xx-5", rows.get(2).getValue("body"));
  }

  @Test
  public void testWithOtherCharacters() throws Exception {
    String[] directives = new String[] {
      "mask-number body xx-xx-TESTING-#"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", 12345)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xx-xx-TESTING-5", rows.get(0).getValue("body"));
  }

  @Test
  public void testWithLong() throws Exception {
    String[] directives = new String[] {
      "mask-number body xx-xx-#"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", 12345L)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xx-xx-5", rows.get(0).getValue("body"));
  }

  @Test
  public void testWithFloat() throws Exception {
    String[] directives = new String[] {
      "mask-number body x#.x#"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", 12.34)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("x2.x4", rows.get(0).getValue("body"));
  }
}

