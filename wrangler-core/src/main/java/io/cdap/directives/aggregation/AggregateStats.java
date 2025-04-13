/*
 * Copyright © 2023-2025 Cask Data, Inc.
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

package io.cdap.directives.aggregation;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * A directive for aggregating byte size and time duration values across multiple rows.
 * This directive allows users to compute the total or average of data sizes and time durations.
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateStats.NAME)
@Categories(categories = {"aggregator"})
@Description("Aggregates columns containing byte sizes and time durations, and computes stats like total or average.")
public class AggregateStats implements Directive, Lineage {

  public static final String NAME = "aggregate-stats";
  private static final String SIZE_COLUMN = "size-column";
  private static final String TIME_COLUMN = "time-column";
  private static final String SIZE_TARGET = "size-target";
  private static final String TIME_TARGET = "time-target";
  private static final String AGGREGATION = "aggregation";
  private static final String SIZE_UNIT = "size-unit";
  private static final String TIME_UNIT = "time-unit";
  
  // Store keys for the aggregation
  private static final String DATA_SIZE_KEY = "aggregate-stats.data-size";
  private static final String TIME_DURATION_KEY = "aggregate-stats.time-duration";
  private static final String ROW_COUNT_KEY = "aggregate-stats.row-count";
  
  // Default units
  private static final String DEFAULT_SIZE_UNIT = "MB";
  private static final String DEFAULT_TIME_UNIT = "s";
  private static final String DEFAULT_AGGREGATION = "total";

  // Column names
  private String sizeColumn;
  private String timeColumn;
  private String sizeTargetColumn;
  private String timeTargetColumn;

  // Configuration
  private String aggregationType;  // "total" or "average"
  private String sizeUnit;
  private String timeUnit;
  
  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define(SIZE_COLUMN, TokenType.COLUMN_NAME);
    builder.define(TIME_COLUMN, TokenType.COLUMN_NAME);
    builder.define(SIZE_TARGET, TokenType.COLUMN_NAME);
    builder.define(TIME_TARGET, TokenType.COLUMN_NAME);
    builder.define(AGGREGATION, TokenType.TEXT, Optional.TRUE);
    builder.define(SIZE_UNIT, TokenType.TEXT, Optional.TRUE);
    builder.define(TIME_UNIT, TokenType.TEXT, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sizeColumn = ((ColumnName) args.value(SIZE_COLUMN)).value();
    this.timeColumn = ((ColumnName) args.value(TIME_COLUMN)).value();
    this.sizeTargetColumn = ((ColumnName) args.value(SIZE_TARGET)).value();
    this.timeTargetColumn = ((ColumnName) args.value(TIME_TARGET)).value();
    
    if (args.contains(AGGREGATION)) {
      this.aggregationType = ((Text) args.value(AGGREGATION)).value();
    } else {
      this.aggregationType = DEFAULT_AGGREGATION;
    }

    if (args.contains(SIZE_UNIT)) {
      this.sizeUnit = ((Text) args.value(SIZE_UNIT)).value().toUpperCase();
    } else {
      this.sizeUnit = DEFAULT_SIZE_UNIT;
    }

    if (args.contains(TIME_UNIT)) {
      this.timeUnit = ((Text) args.value(TIME_UNIT)).value().toLowerCase();
    } else {
      this.timeUnit = DEFAULT_TIME_UNIT;
    }
    
    // Validate size unit
    if (!this.sizeUnit.equals("KB") && !this.sizeUnit.equals("MB") && 
        !this.sizeUnit.equals("GB") && !this.sizeUnit.equals("TB")) {
      throw new DirectiveParseException(
        "Invalid size unit: " + this.sizeUnit + ". Must be KB, MB, GB, or TB.");
    }
    
    // Validate time unit
    if (!this.timeUnit.equals("ms") && !this.timeUnit.equals("s") && 
        !this.timeUnit.equals("min") && !this.timeUnit.equals("h")) {
      throw new DirectiveParseException(
        "Invalid time unit: " + this.timeUnit + ". Must be ms, s, min, or h.");
    }
    
    // Validate aggregation type
    if (!this.aggregationType.equals("total") && !this.aggregationType.equals("average")) {
      throw new DirectiveParseException(
        "Invalid aggregation type: " + this.aggregationType + ". Must be 'total' or 'average'.");
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException, ErrorRowException {
    // Get the transient store for maintaining aggregation state
    TransientStore store = context.getTransientStore();
    
    // Initialize the store if needed
    if (!store.has(TransientVariableScope.DIRECTIVE, DATA_SIZE_KEY)) {
      store.set(TransientVariableScope.DIRECTIVE, DATA_SIZE_KEY, 0L);
      store.set(TransientVariableScope.DIRECTIVE, TIME_DURATION_KEY, 0L);
      store.set(TransientVariableScope.DIRECTIVE, ROW_COUNT_KEY, 0L);
    }
    
    // Retrieve current values
    long totalBytes = store.get(TransientVariableScope.DIRECTIVE, DATA_SIZE_KEY);
    long totalNanos = store.get(TransientVariableScope.DIRECTIVE, TIME_DURATION_KEY);
    long rowCount = store.get(TransientVariableScope.DIRECTIVE, ROW_COUNT_KEY);
    
    // If this is the last batch, compute final aggregates
    if (context.isLast()) {
      // Create result row with aggregated values
      Row result = new Row();
      
      // Calculate the final values based on aggregation type
      double sizeValue = convertBytesToUnit(totalBytes, sizeUnit);
      double timeValue = convertNanosToUnit(totalNanos, timeUnit);
      
      if (this.aggregationType.equals("average") && rowCount > 0) {
        sizeValue = sizeValue / rowCount;
        timeValue = timeValue / rowCount;
      }
      
      // Add the results to the new row
      result.add(sizeTargetColumn, sizeValue);
      result.add(timeTargetColumn, timeValue);
      
      // Return a list with just the result row
      List<Row> results = new ArrayList<>();
      results.add(result);
      return results;
    }
    
    // Process each row for aggregation
    for (Row row : rows) {
      int sizeIdx = row.find(sizeColumn);
      int timeIdx = row.find(timeColumn);
      
      // Skip rows where either column is missing
      if (sizeIdx == -1 || timeIdx == -1) {
        continue;
      }
      
      Object sizeObj = row.getValue(sizeIdx);
      Object timeObj = row.getValue(timeIdx);
      
      if (sizeObj != null) {
        // Parse and accumulate the byte size
        long bytes;
        if (sizeObj instanceof ByteSize) {
          bytes = ((ByteSize) sizeObj).getBytes();
        } else {
          try {
            bytes = parseBytes(sizeObj.toString());
          } catch (ParseException e) {
            throw new ErrorRowException(NAME, 
              "Failed to parse byte size value: " + sizeObj.toString(), 1, e);
          }
        }
        totalBytes += bytes;
      }
      
      if (timeObj != null) {
        // Parse and accumulate the time duration
        long nanos;
        if (timeObj instanceof TimeDuration) {
          nanos = ((TimeDuration) timeObj).getNanoseconds();
        } else {
          try {
            nanos = parseNanos(timeObj.toString());
          } catch (ParseException e) {
            throw new ErrorRowException(NAME, 
              "Failed to parse time duration value: " + timeObj.toString(), 1, e);
          }
        }
        totalNanos += nanos;
      }
      
      // Increment the row count for average calculation
      rowCount++;
    }
    
    // Update the store with new totals
    store.set(TransientVariableScope.DIRECTIVE, DATA_SIZE_KEY, totalBytes);
    store.set(TransientVariableScope.DIRECTIVE, TIME_DURATION_KEY, totalNanos);
    store.set(TransientVariableScope.DIRECTIVE, ROW_COUNT_KEY, rowCount);
    
    // During intermediary processing, don't output any rows
    return new ArrayList<>();
  }

  @Override
  public void destroy() {
    // No resources to clean up
  }
  
  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Aggregated statistics from byte sizes in column '%s' and time durations in column '%s' " +
                "into '%s' and '%s' respectively", sizeColumn, timeColumn, sizeTargetColumn, timeTargetColumn)
      .relation(sizeColumn, sizeTargetColumn)
      .relation(timeColumn, timeTargetColumn)
      .build();
  }
  
  /**
   * Converts bytes to the specified unit.
   *
   * @param bytes Total bytes
   * @param unit Target unit (KB, MB, GB, TB)
   * @return Value in the specified unit
   */
  private double convertBytesToUnit(long bytes, String unit) {
    switch (unit) {
      case "KB":
        return bytes / 1024.0;
      case "MB":
        return bytes / (1024.0 * 1024.0);
      case "GB":
        return bytes / (1024.0 * 1024.0 * 1024.0);
      case "TB":
        return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
      default:
        return bytes;
    }
  }
  
  /**
   * Converts nanoseconds to the specified unit.
   *
   * @param nanos Total nanoseconds
   * @param unit Target unit (ms, s, min, h)
   * @return Value in the specified unit
   */
  private double convertNanosToUnit(long nanos, String unit) {
    switch (unit) {
      case "ms":
        return nanos / 1_000_000.0;
      case "s":
        return nanos / 1_000_000_000.0;
      case "min":
        return nanos / (60.0 * 1_000_000_000.0);
      case "h":
        return nanos / (3600.0 * 1_000_000_000.0);
      default:
        return nanos;
    }
  }
  
  /**
   * Parses a string representation of bytes.
   *
   * @param input String containing a byte size (e.g., "10KB", "1.5MB")
   * @return The parsed size in bytes
   * @throws ParseException If the input cannot be parsed
   */
  private long parseBytes(String input) throws ParseException {
    ByteSize byteSize = new ByteSize(input);
    return byteSize.getBytes();
  }
  
  /**
   * Parses a string representation of time duration.
   *
   * @param input String containing a time duration (e.g., "100ms", "1.5s")
   * @return The parsed duration in nanoseconds
   * @throws ParseException If the input cannot be parsed
   */
  private long parseNanos(String input) throws ParseException {
    TimeDuration duration = new TimeDuration(input);
    return duration.getNanoseconds();
  }
}