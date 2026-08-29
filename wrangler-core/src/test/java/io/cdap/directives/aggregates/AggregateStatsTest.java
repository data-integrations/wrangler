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

package io.cdap.directives.aggregates;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link AggregateStats} implementation.
 */

public class AggregateStatsTest {

  @Test
  public void testBasicSizeAggregation() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("size", "1KB"));
    rows.add(createRow("size", "2KB"));
    rows.add(createRow("size", "3KB"));

    List<Row> results = TestingRig.execute(new String[]{"aggregate-stats :size SIZE"}, rows);
    
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);
    Assert.assertEquals("3KB", result.getValue("sum"));
    Assert.assertEquals("2KB", result.getValue("avg"));
    Assert.assertEquals("1KB", result.getValue("min"));
    Assert.assertEquals("3KB", result.getValue("max"));
  }
  
  @Test
  public void testBasicTimeAggregation() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("duration", "1s"));
    rows.add(createRow("duration", "2s"));
    rows.add(createRow("duration", "3s"));

    List<Row> results = TestingRig.execute(new String[]{"aggregate-stats :duration TIME"}, rows);
    
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);
    Assert.assertEquals("6s", result.getValue("sum"));
    Assert.assertEquals("2s", result.getValue("avg"));
    Assert.assertEquals("1s", result.getValue("min"));
    Assert.assertEquals("3s", result.getValue("max"));
  }
  
  @Test
  public void testDifferentSizeUnits() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("size", "1MB"));
    rows.add(createRow("size", "1024KB"));
    rows.add(createRow("size", "1048576B"));

    List<Row> results = TestingRig.execute(new String[]{"aggregate-stats :size SIZE"}, rows);
    
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);
    Assert.assertEquals("3MB", result.getValue("sum"));
    Assert.assertEquals("1MB", result.getValue("avg"));
    Assert.assertEquals("1MB", result.getValue("min"));
    Assert.assertEquals("1MB", result.getValue("max"));
  }
  
  @Test
  public void testDifferentTimeUnits() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("duration", "1h"));
    rows.add(createRow("duration", "60m"));
    rows.add(createRow("duration", "3600s"));

    List<Row> results = TestingRig.execute(new String[]{"aggregate-stats :duration TIME"}, rows);
    
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);
    Assert.assertEquals("3h", result.getValue("sum"));
    Assert.assertEquals("1h", result.getValue("avg"));
    Assert.assertEquals("1h", result.getValue("min"));
    Assert.assertEquals("1h", result.getValue("max"));
  }
  
  @Test
  public void testBinaryUnits() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("size", "1KiB"));
    rows.add(createRow("size", "1MiB"));
    rows.add(createRow("size", "1GiB"));

    List<Row> results = TestingRig.execute(new String[]{"aggregate-stats :size SIZE"}, rows);
    
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);
    Assert.assertNotNull(result.getValue("sum"));
    Assert.assertNotNull(result.getValue("avg"));
    Assert.assertNotNull(result.getValue("min"));
    Assert.assertNotNull(result.getValue("max"));
  }
  
  @Test(expected = Exception.class)
  public void testInvalidSizeUnit() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("size", "10XB"));

    TestingRig.execute(new String[]{"aggregate-stats :size SIZE"}, rows);
  }
  
  @Test(expected = Exception.class)
  public void testInvalidTimeUnit() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("duration", "10x"));

    TestingRig.execute(new String[]{"aggregate-stats :duration TIME"}, rows);
  }
  
  @Test
  public void testEmptyRows() throws Exception {
    List<Row> rows = new ArrayList<>();
    List<Row> results = TestingRig.execute(new String[]{"aggregate-stats :size SIZE"}, rows);
    Assert.assertEquals(0, results.size());
  }
  
  @Test
  public void testNullValues() throws Exception {
    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("size", null);
    rows.add(row1);
    
    Row row2 = new Row();
    row2.add("size", "10MB");
    rows.add(row2);

    List<Row> results = TestingRig.execute(new String[]{"aggregate-stats :size SIZE"}, rows);
    
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);
    Assert.assertEquals("10MB", result.getValue("sum"));
    Assert.assertEquals("10MB", result.getValue("avg"));
    Assert.assertEquals("10MB", result.getValue("min"));
    Assert.assertEquals("10MB", result.getValue("max"));
  }
  
  /**
   * Helper method to create a row with a single column.
   */
  private Row createRow(String column, String value) {
    Row row = new Row();
    row.add(column, value);
    return row;
  }

}
