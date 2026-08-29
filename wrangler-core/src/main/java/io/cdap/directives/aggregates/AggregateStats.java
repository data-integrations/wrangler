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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Directive for aggregating statistics on byte size and time duration columns.
 *
 * <p>
 * This directive processes columns with size units (B, KB, MB, GB, etc.) or time units (s, m, h, d)
 * and aggregates them into a single row with summary statistics.
 * </p>
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateStats.NAME)
@Categories(categories = {"aggregate"})
@Description("Aggregate statistics on byte size and time duration columns.")
public class AggregateStats implements Directive, Lineage {
  public static final String NAME = "aggregate-stats";
  
  // Size units constants
  private static final String SIZE_TYPE = "SIZE";
  private static final String TIME_TYPE = "TIME";
  
  // Patterns for matching byte sizes and time durations
  private static final Pattern SIZE_PATTERN = Pattern.compile(
      "(\\d+(\\.\\d+)?)\\s*(B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)?",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern TIME_PATTERN = Pattern.compile(
      "(\\d+(\\.\\d+)?)\\s*(ns|μs|ms|s|m|h|d)?",
      Pattern.CASE_INSENSITIVE);
  
  // Maps to convert different units to a base unit (bytes or nanoseconds)
  private static final Map<String, Double> SIZE_MULTIPLIERS = new HashMap<>();
  private static final Map<String, Double> BINARY_SIZE_MULTIPLIERS = new HashMap<>();
  private static final Map<String, Double> TIME_MULTIPLIERS = new HashMap<>();
  
  static {
    // Initialize size multipliers (decimal units)
    SIZE_MULTIPLIERS.put("B", 1.0);
    SIZE_MULTIPLIERS.put("KB", 1024.0);
    SIZE_MULTIPLIERS.put("MB", 1024.0 * 1024.0);
    SIZE_MULTIPLIERS.put("GB", 1024.0 * 1024.0 * 1024.0);
    SIZE_MULTIPLIERS.put("TB", 1024.0 * 1024.0 * 1024.0 * 1024.0);
    SIZE_MULTIPLIERS.put("PB", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
    
    // Initialize binary size multipliers
    BINARY_SIZE_MULTIPLIERS.put("B", 1.0);
    BINARY_SIZE_MULTIPLIERS.put("KIB", 1024.0);
    BINARY_SIZE_MULTIPLIERS.put("MIB", 1024.0 * 1024.0);
    BINARY_SIZE_MULTIPLIERS.put("GIB", 1024.0 * 1024.0 * 1024.0);
    BINARY_SIZE_MULTIPLIERS.put("TIB", 1024.0 * 1024.0 * 1024.0 * 1024.0);
    BINARY_SIZE_MULTIPLIERS.put("PIB", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
    
    // Initialize time multipliers (to nanoseconds)
    TIME_MULTIPLIERS.put("NS", 1.0);
    TIME_MULTIPLIERS.put("US", 1000.0);
    TIME_MULTIPLIERS.put("MS", 1000000.0);
    TIME_MULTIPLIERS.put("S", 1000000000.0);
    TIME_MULTIPLIERS.put("M", 60.0 * 1000000000.0);
    TIME_MULTIPLIERS.put("H", 60.0 * 60.0 * 1000000000.0);
    TIME_MULTIPLIERS.put("D", 24.0 * 60.0 * 60.0 * 1000000000.0);
  }
  
  // Column specifications
  private String column;
  private String type;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("type", TokenType.IDENTIFIER);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.type = ((Identifier) args.value("type")).value();
    
    if (!SIZE_TYPE.equals(type) && !TIME_TYPE.equals(type)) {
      throw new DirectiveParseException(
          NAME, "Invalid column type. Expected SIZE or TIME, but got " + type);
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) 
      throws DirectiveExecutionException, ErrorRowException {
    if (rows.isEmpty()) {
      return rows;
    }
    
    try {
      if ("SIZE".equals(type)) {
        return aggregateSizeValues(rows, column);
      } else if ("TIME".equals(type)) {
        return aggregateTimeValues(rows, column);
      } else {
        throw new DirectiveExecutionException(
            "Invalid aggregation type. Use SIZE or TIME.");
      }
    } catch (Exception e) {
      throw new DirectiveExecutionException(
          "Failed to aggregate statistics: " + e.getMessage(), e);
    }
  }
  
  @Override
  public Mutation lineage() {
    return Mutation.builder()
        .readable("Aggregates statistics for size and time columns")
        .build();
  }
  
  /**
   * Aggregate size values from the specified column.
   *
   * @param rows List of rows to process
   * @param columnName Name of the column containing size values
   * @return The processed rows with aggregated statistics
   * @throws DirectiveExecutionException If there are issues during processing
   */
  private List<Row> aggregateSizeValues(List<Row> rows, String columnName) 
      throws DirectiveExecutionException {
    double sum = 0.0;
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    int count = 0;
    String displayUnit = determineDisplayUnit(rows, columnName, true);
    
    for (Row row : rows) {
      Object value = row.getValue(columnName);
      if (value != null) {
        double bytes = parseSize(value.toString());
        sum += bytes;
        min = Math.min(min, bytes);
        max = Math.max(max, bytes);
        count++;
      }
    }
    
    if (count == 0) {
      return ImmutableList.of();
    }

    double avg = sum / count;
    
    // Create a single row with aggregated statistics
    Row result = new Row();
    double multiplier = getUnitMultiplier(displayUnit, true);
    result.add("sum", formatValue(sum / multiplier) + " " + displayUnit);
    result.add("avg", formatValue(avg / multiplier) + " " + displayUnit);
    result.add("min", formatValue(min / multiplier) + " " + displayUnit);
    result.add("max", formatValue(max / multiplier) + " " + displayUnit);
    
    return ImmutableList.of(result);
  }
  
  /**
   * Aggregate time values from the specified column.
   *
   * @param rows List of rows to process
   * @param columnName Name of the column containing time values
   * @return The processed rows with aggregated statistics
   * @throws DirectiveExecutionException If there are issues during processing
   */
  private List<Row> aggregateTimeValues(List<Row> rows, String columnName) 
      throws DirectiveExecutionException {
    double sum = 0.0;
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    int count = 0;
    String displayUnit = determineDisplayUnit(rows, columnName, false);
    
    for (Row row : rows) {
      Object value = row.getValue(columnName);
      if (value != null) {
        double nanoseconds = parseTime(value.toString());
        sum += nanoseconds;
        min = Math.min(min, nanoseconds);
        max = Math.max(max, nanoseconds);
        count++;
      }
    }
    
    if (count == 0) {
      return ImmutableList.of();
    }

    double avg = sum / count;
    
    // Create a single row with aggregated statistics
    Row result = new Row();
    double multiplier = getUnitMultiplier(displayUnit, false);
    result.add("sum", formatValue(sum / multiplier) + " " + displayUnit);
    result.add("avg", formatValue(avg / multiplier) + " " + displayUnit);
    result.add("min", formatValue(min / multiplier) + " " + displayUnit);
    result.add("max", formatValue(max / multiplier) + " " + displayUnit);
    
    return ImmutableList.of(result);
  }
  
  /**
   * Parse a size string into bytes.
   *
   * @param sizeStr The size string to parse (e.g., "1KB", "2.5MB")
   * @return The size in bytes
   * @throws DirectiveExecutionException If the size string is invalid
   */
  private double parseSize(String sizeStr) throws DirectiveExecutionException {
    Matcher matcher = SIZE_PATTERN.matcher(sizeStr.trim());
    if (!matcher.matches()) {
      throw new DirectiveExecutionException(
          "Invalid size format: " + sizeStr + ". Expected format: number[unit]");
    }
    
    double value = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(3);
    
    if (unit == null || unit.isEmpty()) {
      return value;
    }
    
    String upperUnit = unit.toUpperCase();
    if (SIZE_MULTIPLIERS.containsKey(upperUnit)) {
      return value * SIZE_MULTIPLIERS.get(upperUnit);
    } else if (BINARY_SIZE_MULTIPLIERS.containsKey(upperUnit)) {
      return value * BINARY_SIZE_MULTIPLIERS.get(upperUnit);
    } else {
      throw new DirectiveExecutionException("Invalid size unit: " + unit);
    }
  }
  
  /**
   * Parse a time string into nanoseconds.
   *
   * @param timeStr The time string to parse (e.g., "1s", "2.5m")
   * @return The time in nanoseconds
   * @throws DirectiveExecutionException If the time string is invalid
   */
  private double parseTime(String timeStr) throws DirectiveExecutionException {
    Matcher matcher = TIME_PATTERN.matcher(timeStr.trim());
    if (!matcher.matches()) {
      throw new DirectiveExecutionException(
          "Invalid time format: " + timeStr + ". Expected format: number[unit]");
    }
    
    double value = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(3);
    
    if (unit == null || unit.isEmpty()) {
      return value;
    }
    
    String upperUnit = unit.toUpperCase();
    if (TIME_MULTIPLIERS.containsKey(upperUnit)) {
      return value * TIME_MULTIPLIERS.get(upperUnit);
    } else {
      throw new DirectiveExecutionException("Invalid time unit: " + unit);
    }
  }
  
  /**
   * Determine the most appropriate display unit for the values.
   *
   * @param rows The rows containing the values
   * @param columnName The name of the column
   * @param isSize Whether the values are sizes (true) or times (false)
   * @return The most appropriate display unit
   */
  private String determineDisplayUnit(List<Row> rows, String columnName, boolean isSize) {
    if (rows.isEmpty()) {
      return isSize ? "B" : "s";
    }
    
    // Find the first non-null value
    String firstValue = null;
    for (Row row : rows) {
      Object value = row.getValue(columnName);
      if (value != null) {
        firstValue = value.toString();
        break;
      }
    }
    
    if (firstValue == null) {
      return isSize ? "B" : "s";
    }
    
    // Extract the unit from the first value
    Matcher matcher = isSize ? SIZE_PATTERN.matcher(firstValue) : TIME_PATTERN.matcher(firstValue);
    if (matcher.matches()) {
      String unit = matcher.group(3);
      if (unit != null && !unit.isEmpty()) {
        return unit;
      }
    }
    
    return isSize ? "B" : "s";
  }
  
  /**
   * Get the multiplier for converting from base unit to the specified unit.
   *
   * @param unit The target unit
   * @param isSize Whether the unit is for size (true) or time (false)
   * @return The multiplier to convert from base unit to the specified unit
   */
  private double getUnitMultiplier(String unit, boolean isSize) {
    String upperUnit = unit.toUpperCase();
    if (isSize) {
      if (SIZE_MULTIPLIERS.containsKey(upperUnit)) {
        return SIZE_MULTIPLIERS.get(upperUnit);
      } else if (BINARY_SIZE_MULTIPLIERS.containsKey(upperUnit)) {
        return BINARY_SIZE_MULTIPLIERS.get(upperUnit);
      }
    } else {
      if (TIME_MULTIPLIERS.containsKey(upperUnit)) {
        return TIME_MULTIPLIERS.get(upperUnit);
      }
    }
    return 1.0;
  }
  
  /**
   * Format a numeric value to a string with 2 decimal places.
   *
   * @param value The value to format
   * @return The formatted string
   */
  private String formatValue(double value) {
    return BigDecimal.valueOf(value)
        .setScale(2, RoundingMode.HALF_UP)
        .toString();
  }
} 

