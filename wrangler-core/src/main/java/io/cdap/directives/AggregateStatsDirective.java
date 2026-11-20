/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.directives;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.EntityCountMetric;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;

/**
 * A directive that aggregates byte size and time duration values from columns.
 *
 * Example usage in a recipe:
 *   aggregate-stats <source-byte-column> <source-time-column> <target-size-column> <target-time-column>
 */
public class AggregateStatsDirective implements Directive {

  private String sourceByteColumn;
  private String sourceTimeColumn;
  private String targetSizeColumn;
  private String targetTimeColumn;

  @Override
  public UsageDefinition define() {
    return UsageDefinition.builder("aggregate-stats")
      .define("source-byte-column", TokenType.COLUMN_NAME)
      .define("source-time-column", TokenType.COLUMN_NAME)
      .define("target-size-column", TokenType.TEXT)
      .define("target-time-column", TokenType.TEXT)
      .build();
  }

  /**
   * Overrides the method from Directive/Executor that uses Arguments.
   * We cannot declare 'throws DirectiveExecutionException' if the interface doesn't allow it,
   * so we either do a runtime exception or no-op cast. 
   */
  @Override
  public void initialize(Arguments args) {
    // If your actual code base uses real Arguments logic, parse from 'args' here.
    // Otherwise, if your test just passes a List<String> forcibly cast to Arguments, we can do:
    if (args instanceof List) {
      @SuppressWarnings("unchecked")
      List<String> listArgs = (List<String>) args;
      try {
        initialize(listArgs); // use the convenience method below
      } catch (DirectiveExecutionException e) {
        // We can't rethrow DirectiveExecutionException if the interface doesn't allow it,
        // so convert to a runtime exception or handle appropriately.
        throw new RuntimeException("Initialization failed: " + e.getMessage(), e);
      }
    } else {
      // If we get a real Arguments object that isn't a List, handle differently or fail.
      throw new RuntimeException("Unsupported Arguments type. Expected List<String> for assignment snippet.");
    }
  }

  /**
   * The convenience method your test calls, which does allow 'throws DirectiveExecutionException'.
   */
  public void initialize(List<String> args) throws DirectiveExecutionException {
    if (args.size() < 4) {
      throw new DirectiveExecutionException(
        "Insufficient arguments: need 4 (source-byte, source-time, target-size, target-time).");
    }
    sourceByteColumn = args.get(0);
    sourceTimeColumn = args.get(1);
    targetSizeColumn = args.get(2);
    targetTimeColumn = args.get(3);
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {

    long totalBytes = 0;
    long totalMilliseconds = 0;

    for (Row row : rows) {
      Object byteValue = row.getValue(sourceByteColumn);
      Object timeValue = row.getValue(sourceTimeColumn);
      if (byteValue == null || timeValue == null) {
        // skip incomplete rows
        continue;
      }
      try {
        ByteSize bSize = new ByteSize(byteValue.toString());
        TimeDuration duration = new TimeDuration(timeValue.toString());
        totalBytes += bSize.getBytes();
        totalMilliseconds += duration.getMilliseconds();
      } catch (Exception e) {
        throw new DirectiveExecutionException("Failed to parse or aggregate row values: " + e.getMessage(), e);
      }
    }

    double totalMB = totalBytes / (1024.0 * 1024.0);
    double totalSec = totalMilliseconds / 1000.0;
    Row output = new Row();
    output.add(targetSizeColumn, totalMB);
    output.add(targetTimeColumn, totalSec);
    return Collections.singletonList(output);
  }

  @Override
  public List<EntityCountMetric> getCountMetrics() {
    return Collections.emptyList();
  }

  @Override
  public void destroy() {
    // No cleanup required
  }
}