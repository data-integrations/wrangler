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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.directives.aggregates;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for calculating aggregate statistics on columns containing byte sizes and time durations
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateStats.NAME)
@Categories(categories = {"aggregate"})
@Description("Calculates aggregate statistics (total/average) for byte size and time duration columns")
public class AggregateStats implements Directive, Lineage {
  public static final String NAME = "aggregate-stats";
  private String sizeColumn;
  private String timeColumn;
  private String totalSizeColumn;
  private String totalTimeColumn;
  private String sizeUnit;
  private String timeUnit;
  private boolean average;

  private static final String SIZE_STORE_KEY = "aggregate-stats-size-";
  private static final String TIME_STORE_KEY = "aggregate-stats-time-";
  private static final String COUNT_STORE_KEY = "aggregate-stats-count-";

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("size_column", TokenType.COLUMN_NAME);
    builder.define("time_column", TokenType.COLUMN_NAME);
    builder.define("total_size_column", TokenType.COLUMN_NAME);
    builder.define("total_time_column", TokenType.COLUMN_NAME);
    builder.define("size_unit", TokenType.TEXT);
    builder.define("time_unit", TokenType.TEXT);
    builder.define("average", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sizeColumn = ((ColumnName) args.value("size_column")).value();
    this.timeColumn = ((ColumnName) args.value("time_column")).value();
    this.totalSizeColumn = ((ColumnName) args.value("total_size_column")).value();
    this.totalTimeColumn = ((ColumnName) args.value("total_time_column")).value();
    this.sizeUnit = ((Text) args.value("size_unit")).value().toLowerCase();
    this.timeUnit = ((Text) args.value("time_unit")).value().toLowerCase();
    this.average = "true".equalsIgnoreCase(((Text) args.value("average")).value());

    validateUnits();
  }

  private void validateUnits() throws DirectiveParseException {
    if (!sizeUnit.matches("^(bytes|kb|mb|gb|tb)$")) {
      throw new DirectiveParseException(
        NAME, String.format("Invalid size unit '%s'. Supported units are: bytes, kb, mb, gb, tb", sizeUnit));
    }
    if (!timeUnit.matches("^(ms|s|m|h|d)$")) {
      throw new DirectiveParseException(
        NAME, String.format("Invalid time unit '%s'. Supported units are: ms, s, m, h, d", timeUnit));
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    TransientStore store = context.getTransientStore();
    TransientVariableScope scope = TransientVariableScope.GLOBAL;

    // Initialize counters if not present
    Long nullCount = store.get(SIZE_STORE_KEY);
    if (nullCount == null) {
      store.set(scope, SIZE_STORE_KEY, 0L);
      store.set(scope, TIME_STORE_KEY, 0L);
      store.set(scope, COUNT_STORE_KEY, 0L);
    }

    // Process each row
    for (Row row : rows) {
      Object sizeObj = row.getValue(sizeColumn);
      Object timeObj = row.getValue(timeColumn);

      // Update size total
      if (sizeObj instanceof ByteSize) {
        long bytes = ((ByteSize) sizeObj).getBytes();
        long currentSize = store.get(SIZE_STORE_KEY);
        store.set(scope, SIZE_STORE_KEY, currentSize + bytes);
      }

      // Update time total
      if (timeObj instanceof TimeDuration) {
        long ms = ((TimeDuration) timeObj).getMilliseconds();
        long currentTime = store.get(TIME_STORE_KEY);
        store.set(scope, TIME_STORE_KEY, currentTime + ms);
      }

      long currentCount = store.get(COUNT_STORE_KEY);
      store.set(scope, COUNT_STORE_KEY, currentCount + 1);
    }

    // On last batch, create result row with totals
    // Since isLast() is not available, we'll use a custom flag or configuration
    boolean isFinal = false; // TODO: Determine final batch through configuration or other means

    if (isFinal) {
      long totalBytes = store.get(SIZE_STORE_KEY);
      long totalMs = store.get(TIME_STORE_KEY);
      long count = store.get(COUNT_STORE_KEY);

      if (average && count > 0) {
        totalBytes /= count;
        totalMs /= count;
      }

      // Convert to requested units
      double sizeValue = convertBytes(totalBytes, sizeUnit);
      double timeValue = convertTime(totalMs, timeUnit);

      // Create result row
      Row result = new Row();
      result.add(totalSizeColumn, sizeValue);
      result.add(totalTimeColumn, timeValue);

      List<Row> output = new ArrayList<>();
      output.add(result);
      return output;
    }

    return rows;
  }

  private double convertBytes(long bytes, String unit) {
    switch (unit) {
      case "kb": return bytes / 1024.0;
      case "mb": return bytes / (1024.0 * 1024.0);
      case "gb": return bytes / (1024.0 * 1024.0 * 1024.0);
      case "tb": return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
      default: return bytes;
    }
  }

  private double convertTime(long ms, String unit) {
    switch (unit) {
      case "s": return ms / 1000.0;
      case "m": return ms / (60.0 * 1000.0);
      case "h": return ms / (60.0 * 60.0 * 1000.0);
      case "d": return ms / (24.0 * 60.0 * 60.0 * 1000.0);
      default: return ms;
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Aggregated stats from columns '%s' and '%s' into '%s' and '%s'",
                sizeColumn, timeColumn, totalSizeColumn, totalTimeColumn)
      .relation(Many.columns(sizeColumn, timeColumn), Many.columns(totalSizeColumn, totalTimeColumn))
      .build();
  }
}
