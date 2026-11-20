/*
 * Copyright © 2023 Cask Data, Inc.
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

import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link AggregateStatsDirective}.
 */
public class AggregateStatsDirectiveTest {
  @Mock
  private ExecutorContext context;

  private AggregateStatsDirective directive;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    directive = new AggregateStatsDirective();
  }

  private List<Row> execute(Row row, String... args) throws DirectiveExecutionException, DirectiveParseException {
    directive.initialize(TestUtils.createArguments(args));
    return directive.execute(Arrays.asList(row), context);
  }

  @Test
  public void testBasicAggregation() throws DirectiveExecutionException, DirectiveParseException {
    Row row = new Row();
    row.add("size", new ByteSize("10MB"));
    row.add("duration", new TimeDuration("5s"));

    List<Row> results = execute(row, "size", "duration", "total_mb", "total_sec");
    Row result = results.get(0);

    Assert.assertEquals(10.0, result.getValue("total_mb"), 0.001);
    Assert.assertEquals(5.0, result.getValue("total_sec"), 0.001);
  }

  @Test
  public void testDifferentUnits() throws DirectiveExecutionException, DirectiveParseException {
    Row row = new Row();
    row.add("size", new ByteSize("1GB"));
    row.add("duration", new TimeDuration("2m"));

    List<Row> results = execute(row, "size", "duration", "total_mb", "total_sec");
    Row result = results.get(0);

    Assert.assertEquals(1024.0, result.getValue("total_mb"), 0.001);
    Assert.assertEquals(120.0, result.getValue("total_sec"), 0.001);
  }

  @Test(expected = DirectiveExecutionException.class)
  public void testInvalidByteSize() throws DirectiveExecutionException, DirectiveParseException {
    Row row = new Row();
    row.add("size", "invalid");
    row.add("duration", new TimeDuration("5s"));

    execute(row, "size", "duration", "total_mb", "total_sec");
  }

  @Test(expected = DirectiveExecutionException.class)
  public void testInvalidTimeDuration() throws DirectiveExecutionException, DirectiveParseException {
    Row row = new Row();
    row.add("size", new ByteSize("10MB"));
    row.add("duration", "invalid");

    execute(row, "size", "duration", "total_mb", "total_sec");
  }
} 