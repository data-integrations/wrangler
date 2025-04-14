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

package io.cdap.wrangler.core.directives.aggregate;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELResult;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive that aggregates byte sizes and time durations from source columns into target columns.
 * It can calculate total or average values and optionally convert them to different units.
 */
@Categories(categories = {"aggregate"})
public class SizeTimeAggregator implements Directive {
  private static final String NAME = "size-time-aggregate";
  private static final String TOTAL_SIZE_KEY = "total_size_bytes";
  private static final String TOTAL_TIME_KEY = "total_time_ns";
  private static final String ROW_COUNT_KEY = "row_count";
  
  private ColumnName sourceSizeColumn;
  private ColumnName sourceTimeColumn;
  private ColumnName targetSizeColumn;
  private ColumnName targetTimeColumn;
  private String sizeUnit = "B";  // Default to bytes
  private String timeUnit = "ns"; // Default to nanoseconds
  private boolean calculateAverage = false;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source-size", TokenType.COLUMN_NAME, "Source column containing byte sizes");
    builder.define("source-time", TokenType.COLUMN_NAME, "Source column containing time durations");
    builder.define("target-size", TokenType.COLUMN_NAME, "Target column for aggregated size");
    builder.define("target-time", TokenType.COLUMN_NAME, "Target column for aggregated time");
    builder.define("size-unit", TokenType.TEXT, "Output unit for size (B, KB, MB, GB, TB)", true);
    builder.define("time-unit", TokenType.TEXT, "Output unit for time (ns, ms, s, m, h)", true);
    builder.define("average", TokenType.TEXT, "Calculate average instead of total (true/false)", true);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    sourceSizeColumn = args.value("source-size");
    sourceTimeColumn = args.value("source-time");
    targetSizeColumn = args.value("target-size");
    targetTimeColumn = args.value("target-time");

    if (args.contains("size-unit")) {
      sizeUnit = args.value("size-unit");
      validateSizeUnit(sizeUnit);
    }

    if (args.contains("time-unit")) {
      timeUnit = args.value("time-unit");
      validateTimeUnit(timeUnit);
    }

    if (args.contains("average")) {
      calculateAverage = Boolean.parseBoolean(args.value("average"));
    }
  }

  private void validateSizeUnit(String unit) throws DirectiveParseException {
    if (!unit.matches("(?i)B|KB|MB|GB|TB")) {
      throw new DirectiveParseException(
        NAME, String.format("Invalid size unit '%s'. Must be one of: B, KB, MB, GB, TB", unit));
    }
  }

  private void validateTimeUnit(String unit) throws DirectiveParseException {
    if (!unit.matches("(?i)ns|ms|s|m|h")) {
      throw new DirectiveParseException(
        NAME, String.format("Invalid time unit '%s'. Must be one of: ns, ms, s, m, h", unit));
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    TransientStore store = context.getTransientStore();
    
    // Initialize store if this is the first batch
    if (!store.getVariables().contains(TOTAL_SIZE_KEY)) {
      store.set(TransientVariableScope.GLOBAL, TOTAL_SIZE_KEY, 0L);
      store.set(TransientVariableScope.GLOBAL, TOTAL_TIME_KEY, 0L);
      store.set(TransientVariableScope.GLOBAL, ROW_COUNT_KEY, 0L);
    }

    // Process each row
    for (Row row : rows) {
      int sizeIdx = row.find(sourceSizeColumn.value());
      int timeIdx = row.find(sourceTimeColumn.value());

      if (sizeIdx == -1) {
        throw new DirectiveExecutionException(
          NAME, String.format("Column '%s' not found in row", sourceSizeColumn.value()));
      }
      if (timeIdx == -1) {
        throw new DirectiveExecutionException(
          NAME, String.format("Column '%s' not found in row", sourceTimeColumn.value()));
      }

      try {
        String sizeStr = row.getValue(sizeIdx).toString();
        String timeStr = row.getValue(timeIdx).toString();

        // Parse values to canonical units (bytes and nanoseconds)
        long sizeBytes = parseByteSize(sizeStr);
        long timeNs = parseTimeDuration(timeStr);

        // Update running totals
        store.increment(TransientVariableScope.GLOBAL, TOTAL_SIZE_KEY, sizeBytes);
        store.increment(TransientVariableScope.GLOBAL, TOTAL_TIME_KEY, timeNs);
        store.increment(TransientVariableScope.GLOBAL, ROW_COUNT_KEY, 1L);
      } catch (Exception e) {
        throw new DirectiveExecutionException(
          NAME, String.format("Error parsing values: %s", e.getMessage()), e);
      }
    }

    // Return empty list as we'll handle finalization in destroy()
    return new ArrayList<>();
  }

  @Override
  public void destroy() {
    // This method will be called after all rows are processed
    // The final results will be handled by the RecipePipeline
  }

  private long parseByteSize(String value) {
    // Implementation will use the ByteSize token parser
    // This is a placeholder for the actual implementation
    return 0;
  }

  private long parseTimeDuration(String value) {
    // Implementation will use the TimeDuration token parser
    // This is a placeholder for the actual implementation
    return 0;
  }

  private String formatByteSize(long bytes, String unit) {
    // Implementation will format the bytes into the requested unit
    // This is a placeholder for the actual implementation
    return "";
  }

  private String formatTimeDuration(long nanoseconds, String unit) {
    // Implementation will format the nanoseconds into the requested unit
    // This is a placeholder for the actual implementation
    return "";
  }
} 