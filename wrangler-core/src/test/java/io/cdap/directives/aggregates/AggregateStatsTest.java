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

package io.cdap.directives.aggregates;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the AggregateStats directive.
 */
public class AggregateStatsTest {

  @Test
  public void testTotalAggregation() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row().add("file_size", "1MB").add("duration", "1s"));
    rows.add(new Row().add("file_size", "2MB").add("duration", "2s"));
    rows.add(new Row().add("file_size", "3MB").add("duration", "3s"));

    String[] recipe = new String[] {
      "aggregate-stats :file_size :duration total_size total_time size-unit:MB time-unit:seconds"
    };

    List<Row> results = TestingRig.execute(recipe, rows);
    Assert.assertEquals(1, results.size());
    
    Row result = results.get(0);
    Assert.assertEquals("6MB", result.getValue("total_size"));
    Assert.assertEquals("6s", result.getValue("total_time"));
  }

  @Test
  public void testAverageAggregation() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row().add("file_size", "1MB").add("duration", "1s"));
    rows.add(new Row().add("file_size", "2MB").add("duration", "2s"));
    rows.add(new Row().add("file_size", "3MB").add("duration", "3s"));

    String[] recipe = new String[] {
      "aggregate-stats :file_size :duration total_size total_time " +
      "size-unit:MB time-unit:seconds aggregation-type:average"
    };

    List<Row> results = TestingRig.execute(recipe, rows);
    Assert.assertEquals(1, results.size());
    
    Row result = results.get(0);
    Assert.assertEquals("6MB", result.getValue("total_size"));
    Assert.assertEquals("2s", result.getValue("total_time")); // Average of 1s, 2s, 3s
  }

  @Test
  public void testDifferentUnits() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row().add("file_size", "1024MB").add("duration", "60s"));
    rows.add(new Row().add("file_size", "1024MB").add("duration", "60s"));

    String[] recipe = new String[] {
      "aggregate-stats :file_size :duration total_size total_time size-unit:GB time-unit:minutes"
    };

    List<Row> results = TestingRig.execute(recipe, rows);
    Assert.assertEquals(1, results.size());
    
    Row result = results.get(0);
    Assert.assertEquals("2GB", result.getValue("total_size"));
    Assert.assertEquals("2m", result.getValue("total_time"));
  }

  @Test
  public void testInvalidValues() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row().add("file_size", "1MB").add("duration", "1s"));
    rows.add(new Row().add("file_size", "invalid").add("duration", "invalid"));
    rows.add(new Row().add("file_size", "2MB").add("duration", "2s"));

    String[] recipe = new String[] {
      "aggregate-stats :file_size :duration total_size total_time size-unit:MB time-unit:seconds"
    };

    List<Row> results = TestingRig.execute(recipe, rows);
    Assert.assertEquals(1, results.size());
    
    Row result = results.get(0);
    Assert.assertEquals("3MB", result.getValue("total_size"));
    Assert.assertEquals("3s", result.getValue("total_time"));
  }

  @Test
  public void testInvalidTimeUnit() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row().add("file_size", "1MB").add("duration", "1s"));
    rows.add(new Row().add("file_size", "2MB").add("duration", "2s"));
    rows.add(new Row().add("file_size", "3MB").add("duration", "3s"));

    String[] recipe = new String[] {
      "aggregate-stats :file_size :duration total_size total_time size-unit:MB time-unit:invalid"
    };

    List<Row> results = TestingRig.execute(recipe, rows);
    Assert.assertEquals(1, results.size());
    
    Row result = results.get(0);
    Assert.assertEquals("6MB", result.getValue("total_size"));
    Assert.assertEquals("6s", result.getValue("total_time"));
  }
} 

