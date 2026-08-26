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

package io.cdap.wrangler.api.directive;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class AggregateStatsTest {

  @Mock
  private Arguments args;

  @Mock
  private DirectiveContext context;

  private AggregateStats directive;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    directive = new AggregateStats();
  }

  @Test
  public void testBasicAggregation() throws DirectiveParseException {
    // Setup arguments
    when(args.value("size_column")).thenReturn(new ColumnName(":data_transfer_size"));
    when(args.value("time_column")).thenReturn(new ColumnName(":response_time"));
    when(args.value("total_size_column")).thenReturn(new ColumnName(":total_size_mb"));
    when(args.value("total_time_column")).thenReturn(new ColumnName(":total_time_sec"));

    // Initialize directive
    directive.initialize(args);

    // Create test data
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("data_transfer_size", "1MB", "response_time", "100ms"));
    rows.add(createRow("data_transfer_size", "2MB", "response_time", "200ms"));
    rows.add(createRow("data_transfer_size", "3MB", "response_time", "300ms"));

    // Execute directive
    List<Row> results = directive.execute(rows, context);

    // Verify results
    assertEquals(1, results.size());
    Row result = results.get(0);
    assertEquals(6.0, ((Number)result.getValue("total_size_mb")).doubleValue(), 0.001);
    assertEquals(0.6, ((Number)result.getValue("total_time_sec")).doubleValue(), 0.001);
  }

  @Test
  public void testAverageAggregation() throws DirectiveParseException {
    // Setup arguments
    when(args.value("size_column")).thenReturn(new ColumnName(":data_transfer_size"));
    when(args.value("time_column")).thenReturn(new ColumnName(":response_time"));
    when(args.value("total_size_column")).thenReturn(new ColumnName(":total_size_mb"));
    when(args.value("total_time_column")).thenReturn(new ColumnName(":total_time_sec"));
    when(args.contains("aggregation_type")).thenReturn(true);
    when(args.value("aggregation_type")).thenReturn(new Text("average"));

    // Initialize directive
    directive.initialize(args);

    // Create test data
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("data_transfer_size", "1MB", "response_time", "100ms"));
    rows.add(createRow("data_transfer_size", "2MB", "response_time", "200ms"));
    rows.add(createRow("data_transfer_size", "3MB", "response_time", "300ms"));

    // Execute directive
    List<Row> results = directive.execute(rows, context);

    // Verify results
    assertEquals(1, results.size());
    Row result = results.get(0);
    assertEquals(2.0, ((Number)result.getValue("total_size_mb")).doubleValue(), 0.001);
    assertEquals(0.2, ((Number)result.getValue("total_time_sec")).doubleValue(), 0.001);
  }

  @Test
  public void testDifferentUnits() throws DirectiveParseException {
    // Setup arguments
    when(args.value("size_column")).thenReturn(new ColumnName(":data_transfer_size"));
    when(args.value("time_column")).thenReturn(new ColumnName(":response_time"));
    when(args.value("total_size_column")).thenReturn(new ColumnName(":total_size_gb"));
    when(args.value("total_time_column")).thenReturn(new ColumnName(":total_time_min"));
    when(args.contains("size_unit")).thenReturn(true);
    when(args.value("size_unit")).thenReturn(new Text("GB"));
    when(args.contains("time_unit")).thenReturn(true);
    when(args.value("time_unit")).thenReturn(new Text("min"));

    // Initialize directive
    directive.initialize(args);

    // Create test data
    List<Row> rows = new ArrayList<>();
    rows.add(createRow("data_transfer_size", "1024MB", "response_time", "60s"));
    rows.add(createRow("data_transfer_size", "2048MB", "response_time", "120s"));

    // Execute directive
    List<Row> results = directive.execute(rows, context);

    // Verify results
    assertEquals(1, results.size());
    Row result = results.get(0);
    assertEquals(3.0, ((Number)result.getValue("total_size_gb")).doubleValue(), 0.001);
    assertEquals(3.0, ((Number)result.getValue("total_time_min")).doubleValue(), 0.001);
  }

  private Row createRow(String sizeCol, String sizeVal, String timeCol, String timeVal) {
    Row row = new Row();
    row.add(sizeCol, sizeVal);
    row.add(timeCol, timeVal);
    return row;
  }
} 