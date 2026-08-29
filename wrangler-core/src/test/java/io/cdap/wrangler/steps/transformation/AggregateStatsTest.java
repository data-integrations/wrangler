/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.wrangler.steps.transformation;

import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * Tests for the {@link AggregateStats} directive.
 */
public class AggregateStatsTest {

  @Mock
  private ExecutorContext executorContext;

  @Mock
  private DirectiveContext directiveContext;

  private AggregateStats directive;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    directive = new AggregateStats();
    directive.initialize(executorContext);
  }

  @Test
  public void testBasicAggregation() throws DirectiveParseException, DirectiveExecutionException {
    // Configure the directive
    Map<String, String> arguments = new HashMap<>();
    arguments.put("column", "size");
    arguments.put("operation", "sum");
    directive.configure(arguments);

    // Create test data
    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("size", new ByteSize(1024, "B", "1024B"));
    rows.add(row1);

    Row row2 = new Row();
    row2.add("size", new ByteSize(1, "KB", "1KB"));
    rows.add(row2);

    // Execute the directive
    List<Row> result = directive.execute(rows, directiveContext);

    // Verify the result
    Assert.assertEquals(2, result.size());
    ByteSize expectedSum = new ByteSize(2048, "B", "2048B");
    Assert.assertEquals(expectedSum, result.get(0).getValue("size_sum"));
    Assert.assertEquals(expectedSum, result.get(1).getValue("size_sum"));
  }

  @Test
  public void testDifferentUnits() throws DirectiveParseException, DirectiveExecutionException {
    // Configure the directive
    Map<String, String> arguments = new HashMap<>();
    arguments.put("column", "duration");
    arguments.put("operation", "avg");
    directive.configure(arguments);

    // Create test data
    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("duration", new TimeDuration(1000, "ms", "1000ms"));
    rows.add(row1);

    Row row2 = new Row();
    row2.add("duration", new TimeDuration(1, "s", "1s"));
    rows.add(row2);

    // Execute the directive
    List<Row> result = directive.execute(rows, directiveContext);

    // Verify the result
    Assert.assertEquals(2, result.size());
    TimeDuration expectedAvg = new TimeDuration(1500, "ms", "1500ms");
    Assert.assertEquals(expectedAvg, result.get(0).getValue("duration_avg"));
    Assert.assertEquals(expectedAvg, result.get(1).getValue("duration_avg"));
  }

  @Test
  public void testMixedUnits() throws DirectiveParseException, DirectiveExecutionException {
    // Configure the directive
    Map<String, String> arguments = new HashMap<>();
    arguments.put("column", "size");
    arguments.put("operation", "max");
    directive.configure(arguments);

    // Create test data
    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("size", new ByteSize(1024, "B", "1024B"));
    rows.add(row1);

    Row row2 = new Row();
    row2.add("size", new ByteSize(1, "MB", "1MB"));
    rows.add(row2);

    Row row3 = new Row();
    row3.add("size", new ByteSize(512, "KB", "512KB"));
    rows.add(row3);

    // Execute the directive
    List<Row> result = directive.execute(rows, directiveContext);

    // Verify the result
    Assert.assertEquals(3, result.size());
    ByteSize expectedMax = new ByteSize(1, "MB", "1MB");
    Assert.assertEquals(expectedMax, result.get(0).getValue("size_max"));
    Assert.assertEquals(expectedMax, result.get(1).getValue("size_max"));
    Assert.assertEquals(expectedMax, result.get(2).getValue("size_max"));
  }

  @Test(expected = DirectiveExecutionException.class)
  public void testInvalidSizeFormat() throws DirectiveParseException, DirectiveExecutionException {
    // Configure the directive
    Map<String, String> arguments = new HashMap<>();
    arguments.put("column", "size");
    arguments.put("operation", "sum");
    directive.configure(arguments);

    // Create test data with invalid format
    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("size", "invalid");
    rows.add(row1);

    // Execute the directive - should throw an exception
    directive.execute(rows, directiveContext);
  }

  @Test(expected = DirectiveExecutionException.class)
  public void testInvalidTimeFormat() throws DirectiveParseException, DirectiveExecutionException {
    // Configure the directive
    Map<String, String> arguments = new HashMap<>();
    arguments.put("column", "duration");
    arguments.put("operation", "sum");
    directive.configure(arguments);

    // Create test data with invalid format
    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("duration", "invalid");
    rows.add(row1);

    // Execute the directive - should throw an exception
    directive.execute(rows, directiveContext);
  }
} 