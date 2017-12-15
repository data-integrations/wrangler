/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.row;

import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.ErrorRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SendToError}
 */
public class SendToErrorTest {

  @Test
  public void testErrorConditionTrue() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "send-to-error C == 1",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "A,B,C,D"),
      new Row("body", "X,Y,1,2.0"),
      new Row("body", "U,V,2,3.0")
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("2.0", errors.get(0).getRow().getValue("D"));
    Assert.assertEquals("2", results.get(0).getValue("C"));
  }

  @Test
  public void testRegexFiltering() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "send-to-error A =~ \"Was.*\"",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "A,B"),
      new Row("body", "Washington,Y"),
      new Row("body", "Window,V")
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(1, results.size());
  }

  @Test
  public void testNullFieldsSkipping() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "send-to-error exp:{C1 =~ \"Was.*\"};",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "A,B,C"),
      new Row("body", "Washington,Y"),
      new Row("body", "Window,V,XYZ")
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(0, errors.size());
    Assert.assertEquals(2, results.size());
  }

  @Test
  public void testIntegerField() throws Exception {
    String[] directives = new String[] {
      "send-to-error field_calories_cnt < 0"
    };

    List<Row> rows = Arrays.asList(
      new Row("field_calories_cnt", new Integer(10)),
      new Row("field_calories_cnt", new Integer(0)),
      new Row("field_calories_cnt", new Integer(-10))
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(2, results.size());
  }

  @Test
  public void testMissingVariables() throws Exception {
    String[] directives = new String[] {
      "send-to-error field_calories_cnt < 0 && field_not_exist == 'test'"
    };

    List<Row> rows = Arrays.asList(
      new Row("field_calories_cnt", new Integer(10)),
      new Row("field_calories_cnt", new Integer(0)),
      new Row("field_calories_cnt", new Integer(-10))
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(0, errors.size());
    Assert.assertEquals(3, results.size());
  }

  @Test
  public void testSendToErrorWithMessage() throws Exception {
    String[] directives = new String[] {
      "send-to-error exp:{field_calories_cnt < 0} 'Test Message';"
    };

    List<Row> rows = Arrays.asList(
      new Row("field_calories_cnt", new Integer(10)),
      new Row("field_calories_cnt", new Integer(0)),
      new Row("field_calories_cnt", new Integer(-10))
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(1, errors.size());
    Assert.assertEquals("Test Message", errors.get(0).getMessage());
    Assert.assertEquals(2, results.size());
  }

  @Test
  public void testSendToErrorWithMetricAndMessage() throws Exception {
    String[] directives = new String[] {
      "send-to-error exp:{field_calories_cnt < 0} test 'Test Message';"
    };

    List<Row> rows = Arrays.asList(
      new Row("field_calories_cnt", new Integer(10)),
      new Row("field_calories_cnt", new Integer(0)),
      new Row("field_calories_cnt", new Integer(-10))
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(1, errors.size());
    Assert.assertEquals("Test Message", errors.get(0).getMessage());
    Assert.assertEquals(2, results.size());
  }

  @Test
  public void testSendToErrorWithMetric() throws Exception {
    String[] directives = new String[] {
      "send-to-error exp:{field_calories_cnt < 0} test;"
    };

    List<Row> rows = Arrays.asList(
      new Row("field_calories_cnt", new Integer(10)),
      new Row("field_calories_cnt", new Integer(0)),
      new Row("field_calories_cnt", new Integer(-10))
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(2, results.size());
  }
}
