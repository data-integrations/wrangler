/*
 *  Copyright © 2021 Cask Data, Inc.
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
package io.cdap.directives.datetime;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CurrentDateTimeTest {

  @Test
  public void testDefaultZone() throws Exception {
    String colName = "col1";
    String[] directives = new String[]{
      String.format("%s :%s", CurrentDateTime.NAME, colName)
    };
    Row row1 = new Row();
    row1.add(colName, null);
    List<Row> result = TestingRig.execute(directives, Collections.singletonList(row1));
    Assert.assertTrue(result.get(0).getValue(colName) instanceof LocalDateTime);
  }

  @Test
  public void testAddColumn() throws Exception {
    String colName = "col1";
    String[] directives = new String[]{
      String.format("%s :%s", CurrentDateTime.NAME, colName)
    };
    //Skip column - it should be automatically added
    Row row1 = new Row();
    Row row2 = new Row();
    List<Row> result = TestingRig.execute(directives, Arrays.asList(row1, row2));
    Row resultRow1 = result.get(0);
    Assert.assertEquals(1, resultRow1.width());
    Assert.assertTrue(resultRow1.getValue(colName) instanceof LocalDateTime);
    Row resultRow2 = result.get(1);
    Assert.assertEquals(1, resultRow2.width());
    Assert.assertTrue(resultRow2.getValue(colName) instanceof LocalDateTime);
  }

  @Test(expected = RecipeException.class)
  public void testInvalidZone() throws Exception {
    String zone = "abcd";
    String colName = "col1";
    String[] directives = new String[]{String.format("%s :%s '%s'", CurrentDateTime.NAME, colName, zone)};
    Row row1 = new Row();
    row1.add(colName, null);
    TestingRig.execute(directives, Collections.singletonList(row1));
  }
}
