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

package io.cdap.wrangler.parser;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Aggregator;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenGroup;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A directive that aggregates byte size and time duration columns.
 */
@Plugin(type = Directive.TYPE)
@Name("aggregate-stats")
@Categories(categories = { "aggregator" })
@Description("Aggregates byte size and time duration columns into total size and total time")
public class AggregateStats implements Directive, Aggregator {
  public static final String NAME = "aggregate-stats";
  private String sourceSizeColumn;
  private String sourceTimeColumn;
  private String targetSizeColumn;
  private String targetTimeColumn;
  private String sizeUnit = "MB"; // Default output unit for size
  private String timeUnit = "s";  // Default output unit for time

  private static final String TOTAL_SIZE_KEY = "total_size";
  private static final String TOTAL_TIME_KEY = "total_time";
  private static final String COUNT_KEY = "count";

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source-size-column", TokenType.COLUMN_NAME);
    builder.define("source-time-column", TokenType.COLUMN_NAME);
    builder.define("target-size-column", TokenType.COLUMN_NAME);
    builder.define("target-time-column", TokenType.COLUMN_NAME);
    builder.define("size-unit", TokenType.IDENTIFIER, Optional.TRUE);
    builder.define("time-unit", TokenType.IDENTIFIER, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(TokenGroup tokenGroup) throws DirectiveParseException {
    this.sourceSizeColumn = ((ColumnName) tokenGroup.get("source-size-column")).value();
    this.sourceTimeColumn = ((ColumnName) tokenGroup.get("source-time-column")).value();
    this.targetSizeColumn = ((ColumnName) tokenGroup.get("target-size-column")).value();
    this.targetTimeColumn = ((ColumnName) tokenGroup.get("target-time-column")).value();
    
    if (tokenGroup.contains("size-unit")) {
      this.sizeUnit = ((Identifier) tokenGroup.get("size-unit")).value();
      // Validate size unit
      if (!isValidSizeUnit(this.sizeUnit)) {
        throw new DirectiveParseException(
          String.format("Invalid size unit: '%s'. Expected one of: B, KB, MB, GB, TB, PB", this.sizeUnit));
      }
    }
    
    if (tokenGroup.contains("time-unit")) {
      this.timeUnit = ((Identifier) tokenGroup.get("time-unit")).value();
      // Validate time unit
      if (!isValidTimeUnit(this.timeUnit)) {
        throw new DirectiveParseException(
          String.format("Invalid time unit: '%s'. Expected one of: ns, ms, s, m, h, d", this.timeUnit));
      }
    }
  }

  @Override
  public void destroy() {
    // No-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    // This method will be called for each batch of rows
    Map<String, Object> store = context.getTransientStore();
    
    // Initialize store if not already initialized
    if (!store.containsKey(TOTAL_SIZE_KEY)) {
      store.put(TOTAL_SIZE_KEY, 0L);
      store.put(TOTAL_TIME_KEY, 0L);
      store.put(COUNT_KEY, 0L);
    }
    
    // Aggregate data
    for (Row row : rows) {
      // Process byte size
      if (row.find(sourceSizeColumn) != -1) {
        Object sizeObj = row.getValue(sourceSizeColumn);
        try {
          long sizeInBytes = convertToBytes(sizeObj);
          store.put(TOTAL_SIZE_KEY, (long) store.get(TOTAL_SIZE_KEY) + sizeInBytes);
        } catch (Exception e) {
          throw new DirectiveExecutionException(
            String.format("Error processing byte size in column '%s': %s", sourceSizeColumn, e.getMessage()), e);
        }
      }
      
      // Process time duration
      if (row.find(sourceTimeColumn) != -1) {
        Object timeObj = row.getValue(sourceTimeColumn);
        try {
          long timeInNanos = convertToNanos(timeObj);
          store.put(TOTAL_TIME_KEY, (long) store.get(TOTAL_TIME_KEY) + timeInNanos);
        } catch (Exception e) {
          throw new DirectiveExecutionException(
            String.format("Error processing time duration in column '%s': %s", sourceTimeColumn, e.getMessage()), e);
        }
      }
      
      // Increment count
      store.put(COUNT_KEY, (long) store.get(COUNT_KEY) + 1);
    }
    
    // For normal execution, return the input rows unchanged
    return rows;
  }

  @Override
  public List<Row> aggregate(ExecutorContext context) throws DirectiveExecutionException {
    Map<String, Object> store = context.getTransientStore();
    
    // If no data has been processed, return empty result
    if (!store.containsKey(TOTAL_SIZE_KEY)) {
      return new ArrayList<>();
    }
    
    long totalSizeBytes = (long) store.get(TOTAL_SIZE_KEY);
    long totalTimeNanos = (long) store.get(TOTAL_TIME_KEY);
    long count = (long) store.get(COUNT_KEY);
    
    // Convert to requested units
    double convertedSize = convertByteSize(totalSizeBytes, sizeUnit);
    double convertedTime = convertTimeDuration(totalTimeNanos, timeUnit);
    
    // Create result row
    Row resultRow = new Row();
    resultRow.add(targetSizeColumn, convertedSize);
    resultRow.add(targetTimeColumn, convertedTime);
    
    // Cleanup store
    store.remove(TOTAL_SIZE_KEY);
    store.remove(TOTAL_TIME_KEY);
    store.remove(COUNT_KEY);
    
    List<Row> results = new ArrayList<>();
    results.add(resultRow);
    return results;
  }

  @Override
  public Mutation lineage() {
    return Lineage.builder()
      .readColumn(sourceSizeColumn)
      .readColumn(sourceTimeColumn)
      .writeColumn(targetSizeColumn)
      .writeColumn(targetTimeColumn)
      .build();
  }

  /**
   * Converts an object to bytes. Handles ByteSize objects, strings, and numbers.
   *
   * @param obj The object to convert
   * @return The size in bytes
   */
  private long convertToBytes(Object obj) {
    if (obj instanceof ByteSize) {
      return ((ByteSize) obj).getBytes();
    } else if (obj instanceof String) {
      try {
        return new ByteSize((String) obj).getBytes();
      } catch (Exception e) {
        // Try to parse as a number
        try {
          return Long.parseLong((String) obj);
        } catch (NumberFormatException nfe) {
          throw new IllegalArgumentException(
            String.format("Cannot convert '%s' to bytes", obj));
        }
      }
    } else if (obj instanceof Number) {
      return ((Number) obj).longValue();
    } else {
      throw new IllegalArgumentException(
        String.format("Cannot convert object of type '%s' to bytes", obj.getClass().getName()));
    }
  }

  /**
   * Converts an object to nanoseconds. Handles TimeDuration objects, strings, and numbers.
   *
   * @param obj The object to convert
   * @return The duration in nanoseconds
   */
  private long convertToNanos(Object obj) {
    if (obj instanceof TimeDuration) {
      return ((TimeDuration) obj).getNanoseconds();
    } else if (obj instanceof String) {
      try {
        return new TimeDuration((String) obj).getNanoseconds();
      } catch (Exception e) {
        // Try to parse as a number
        try {
          return Long.parseLong((String) obj);
        } catch (NumberFormatException nfe) {
          throw new IllegalArgumentException(
            String.format("Cannot convert '%s' to nanoseconds", obj));
        }
      }
    } else if (obj instanceof Number) {
      return ((Number) obj).longValue();
    } else {
      throw new IllegalArgumentException(
        String.format("Cannot convert object of type '%s' to nanoseconds", obj.getClass().getName()));
    }
  }

  /**
   * Converts a byte size from bytes to the specified unit.
   *
   * @param bytes The size in bytes
   * @param unit The target unit
   * @return The converted size
   */
  private double convertByteSize(long bytes, String unit) {
    switch (unit.toUpperCase()) {
      case "B":
        return bytes;
      case "KB":
        return bytes / 1024.0;
      case "MB":
        return bytes / (1024.0 * 1024.0);
      case "GB":
        return bytes / (1024.0 * 1024.0 * 1024.0);
      case "TB":
        return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
      case "PB":
        return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
      default:
        throw new IllegalArgumentException("Invalid size unit: " + unit);
    }
  }

  /**
   * Converts a time duration from nanoseconds to the specified unit.
   *
   * @param nanos The duration in nanoseconds
   * @param unit The target unit
   * @return The converted duration
   */
  private double convertTimeDuration(long nanos, String unit) {
    switch (unit) {
      case "ns":
        return nanos;
      case "ms":
        return nanos / 1_000_000.0;
      case "s":
        return nanos / 1_000_000_000.0;
      case "m":
        return nanos / (60.0 * 1_000_000_000.0);
      case "h":
        return nanos / (60.0 * 60.0 * 1_000_000_000.0);
      case "d":
        return nanos / (24.0 * 60.0 * 60.0 * 1_000_000_000.0);
      default:
        throw new IllegalArgumentException("Invalid time unit: " + unit);
    }
  }

  /**
   * Checks if the given size unit is valid.
   *
   * @param unit The unit to check
   * @return True if valid, false otherwise
   */
  private boolean isValidSizeUnit(String unit) {
    if (unit == null) {
      return false;
    }
    String upperUnit = unit.toUpperCase();
    return upperUnit.equals("B") || upperUnit.equals("KB") || upperUnit.equals("MB") ||
           upperUnit.equals("GB") || upperUnit.equals("TB") || upperUnit.equals("PB");
  }

  /**
   * Checks if the given time unit is valid.
   *
   * @param unit The unit to check
   * @return True if valid, false otherwise
   */
  private boolean isValidTimeUnit(String unit) {
    return unit != null && (unit.equals("ns") || unit.equals("ms") || unit.equals("s") ||
                           unit.equals("m") || unit.equals("h") || unit.equals("d"));
  }
}