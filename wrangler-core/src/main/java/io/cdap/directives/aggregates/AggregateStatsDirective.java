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

 import com.google.common.collect.ImmutableMap;
 import io.cdap.cdap.api.annotation.Description;
 import io.cdap.cdap.api.annotation.Name;
 import io.cdap.cdap.api.annotation.Plugin;
 import io.cdap.wrangler.api.Arguments;
 import io.cdap.wrangler.api.Directive;
 import io.cdap.wrangler.api.DirectiveExecutionException;
 import io.cdap.wrangler.api.DirectiveParseException;
 import io.cdap.wrangler.api.ExecutorContext;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.annotations.Categories;
 import io.cdap.wrangler.api.lineage.Lineage;
 import io.cdap.wrangler.api.lineage.Mutation;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.Identifier;
 import io.cdap.wrangler.api.parser.Token;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 
 import java.math.BigDecimal;
 import java.math.RoundingMode;
 import java.util.Collections;
 import java.util.List;
 import java.util.Map;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 
 /**
  * A directive that aggregates statistics for size and time duration columns.
  * It calculates sum, average, minimum, maximum, and returns a count value of 1 (representing
  * the single aggregated row) for the specified column.
  * For size columns, it supports units like B, KB, MB, GB, TB, PB, KiB, MiB, GiB, TiB, PiB.
  * For time columns, it supports units like ns, μs, ms, s, m, h, d.
  */
 @Plugin(type = Directive.TYPE)
 @Name(AggregateStatsDirective.NAME)
 @Categories(categories = {"aggregate"})
 @Description("Aggregate statistics on byte size and time duration columns.")
 public class AggregateStatsDirective implements Directive, Lineage {
   public static final String NAME = "aggregate-stats";
 
   private static final Pattern SIZE_PATTERN = Pattern.compile(
     "(\\d+(\\.\\d+)?)\\s*(B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)?",
     Pattern.CASE_INSENSITIVE);
 
   private static final Pattern TIME_PATTERN = Pattern.compile(
     "(\\d+(\\.\\d+)?)\\s*(ns|μs|ms|s|m|h|d)?",
     Pattern.CASE_INSENSITIVE);
 
   private static final Map<String, Double> SIZE_MULTIPLIERS = ImmutableMap.<String, Double>builder()
     .put("B", 1.0)
     .put("KB", 1024.0)
     .put("MB", Math.pow(1024, 2))
     .put("GB", Math.pow(1024, 3))
     .put("TB", Math.pow(1024, 4))
     .put("PB", Math.pow(1024, 5))
     .build();
 
   private static final Map<String, Double> BINARY_SIZE_MULTIPLIERS = ImmutableMap.<String, Double>builder()
     .put("KIB", 1024.0)
     .put("MIB", Math.pow(1024, 2))
     .put("GIB", Math.pow(1024, 3))
     .put("TIB", Math.pow(1024, 4))
     .put("PIB", Math.pow(1024, 5))
     .build();
 
   private static final Map<String, Double> TIME_MULTIPLIERS = ImmutableMap.<String, Double>builder()
     .put("NS", 1.0)
     .put("μS", 1000.0)
     .put("MS", 1_000_000.0)
     .put("S", 1_000_000_000.0)
     .put("M", 60.0 * 1_000_000_000)
     .put("H", 3600.0 * 1_000_000_000)
     .put("D", 86400.0 * 1_000_000_000)
     .build();
 
   private String column;
   private String type;
   private String outputUnit;
   private Arguments arguments;
 
   // Holds intermediate aggregation values.
   private static class AggregationState {
     double sum = 0;
     double min = Double.MAX_VALUE;
     double max = Double.MIN_VALUE;
     int count = 0;
   }
 
   @Override
   public UsageDefinition define() {
     UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
     builder.define("column", TokenType.COLUMN_NAME);
     builder.define("type", TokenType.IDENTIFIER);
     builder.define("output_unit", TokenType.IDENTIFIER, true); // Optional output unit
     return builder.build();
   }
 
   @Override
   public void initialize(Arguments args) throws DirectiveParseException {
     this.arguments = args;
     this.column = ((ColumnName) args.value("column")).value();
     this.type = ((Identifier) args.value("type")).value();
     this.outputUnit = args.contains("output_unit") ? 
         ((Identifier) args.value("output_unit")).value() : null;

     if (!type.equalsIgnoreCase("SIZE") && !type.equalsIgnoreCase("DURATION")) {
       throw new DirectiveParseException(NAME, "Invalid type. Expected SIZE or DURATION.");
     }

     if (outputUnit != null) {
       if (type.equalsIgnoreCase("SIZE")) {
         if (!isValidSizeUnit(outputUnit)) {
           throw new DirectiveParseException(NAME, "Invalid size unit: " + outputUnit);
         }
       } else {
         if (!isValidTimeUnit(outputUnit)) {
           throw new DirectiveParseException(NAME, "Invalid time unit: " + outputUnit);
         }
       }
     }
   }

   private boolean isValidSizeUnit(String unit) {
     String upperUnit = unit.toUpperCase();
     return SIZE_MULTIPLIERS.containsKey(upperUnit) || BINARY_SIZE_MULTIPLIERS.containsKey(upperUnit);
   }

   private boolean isValidTimeUnit(String unit) {
     return TIME_MULTIPLIERS.containsKey(unit.toUpperCase());
   }
 
   @Override
   public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
     AggregationState state = new AggregationState();
     String typeLower = type.toLowerCase();
 
     // For each row, try to parse the value and aggregate.
     for (Row row : rows) {
       Object val = row.getValue(column);
       if (val == null) {
         continue;
       }
 
       String strVal = val.toString();
       double parsed;
       try {
         if (typeLower.equals("size")) {
           parsed = parseSize(strVal);
         } else {
           parsed = parseTime(strVal);
         }
         state.sum += parsed;
         state.min = Math.min(state.min, parsed);
         state.max = Math.max(state.max, parsed);
         state.count++;
       } catch (IllegalArgumentException e) {
         throw new DirectiveExecutionException(NAME, e.getMessage());
       }
     }
 
     // Create a single aggregate row.
     Row result = new Row();
     if (state.count == 0) {
       // When no valid values found, use default values.
       if (typeLower.equals("size")) {
         result.add("sum", formatSize(0, outputUnit));
         result.add("avg", formatSize(0, outputUnit));
         result.add("min", formatSize(0, outputUnit));
         result.add("max", formatSize(0, outputUnit));
       } else {
         result.add("sum", formatTime(0, outputUnit));
         result.add("avg", formatTime(0, outputUnit));
         result.add("min", formatTime(0, outputUnit));
         result.add("max", formatTime(0, outputUnit));
       }
     } else {
       double avg = state.sum / state.count;
       if (typeLower.equals("size")) {
         result.add("sum", formatSize(state.sum, outputUnit));
         result.add("avg", formatSize(avg, outputUnit));
         result.add("min", formatSize(state.min, outputUnit));
         result.add("max", formatSize(state.max, outputUnit));
       } else {
         result.add("sum", formatTime(state.sum, outputUnit));
         result.add("avg", formatTime(avg, outputUnit));
         result.add("min", formatTime(state.min, outputUnit));
         result.add("max", formatTime(state.max, outputUnit));
       }
     }
 
     // Regardless of how many rows were aggregated, always set count to 1.
     result.add("count", 1);
     return Collections.singletonList(result);
   }
 
   /**
    * Parses a size string (e.g., "1KB", "2.5MB") into bytes.
    * Throws IllegalArgumentException if the format or unit is invalid.
    */
   private double parseSize(String value) {
     Matcher matcher = SIZE_PATTERN.matcher(value);
     if (!matcher.matches()) {
       throw new IllegalArgumentException("Invalid size format: " + value);
     }
 
     double number = Double.parseDouble(matcher.group(1));
     String unit = (matcher.group(3) != null) ? matcher.group(3).toUpperCase() : "B";
 
     Double multiplier = SIZE_MULTIPLIERS.get(unit);
     if (multiplier == null) {
       multiplier = BINARY_SIZE_MULTIPLIERS.get(unit);
       if (multiplier == null) {
         throw new IllegalArgumentException("Invalid size unit: " + unit);
       }
     }
     return number * multiplier;
   }
 
   /**
    * Parses a time string (e.g., "1s", "2.5m") into nanoseconds.
    * Throws IllegalArgumentException if the format or unit is invalid.
    */
   private double parseTime(String value) {
     Matcher matcher = TIME_PATTERN.matcher(value);
     if (!matcher.matches()) {
       throw new IllegalArgumentException("Invalid time format: " + value);
     }
 
     double number = Double.parseDouble(matcher.group(1));
     String unit = (matcher.group(3) != null) ? matcher.group(3).toUpperCase() : "S";
 
     Double multiplier = TIME_MULTIPLIERS.get(unit);
     if (multiplier == null) {
       throw new IllegalArgumentException("Invalid time unit: " + unit);
     }
     return number * multiplier;
   }
 
   /**
    * Converts a byte value into a human-readable string (e.g., "1.23MB").
    */
   private String formatSize(double value, String outputUnit) {
     if (outputUnit != null) {
       double multiplier = getSizeMultiplier(outputUnit);
       return String.format("%.2f%s", value / multiplier, outputUnit);
     }

     String[] units = {"B", "KB", "MB", "GB", "TB", "PB"};
     int idx = 0;
     while (value >= 1024 && idx < units.length - 1) {
       value /= 1024;
       idx++;
     }
     return String.format("%.2f%s", value, units[idx]);
   }
 
   /**
    * Converts a time value in nanoseconds to a human-readable string (e.g., "1.23s").
    */
   private String formatTime(double value, String outputUnit) {
     if (outputUnit != null) {
       double multiplier = getTimeMultiplier(outputUnit);
       return String.format("%.2f%s", value / multiplier, outputUnit);
     }

     String[] units = {"ns", "μs", "ms", "s", "m", "h", "d"};
     double[] multipliers = {1, 1_000.0, 1_000_000.0, 1_000_000_000.0,
                             60.0 * 1_000_000_000.0, 3600.0 * 1_000_000_000.0,
                             86400.0 * 1_000_000_000.0};

     int idx = units.length - 1;
     while (idx > 0 && value < multipliers[idx]) {
       idx--;
     }
     return String.format("%.2f%s", value / multipliers[idx], units[idx]);
   }
 
   private double getSizeMultiplier(String unit) {
     String upperUnit = unit.toUpperCase();
     Double multiplier = SIZE_MULTIPLIERS.get(upperUnit);
     if (multiplier == null) {
       multiplier = BINARY_SIZE_MULTIPLIERS.get(upperUnit);
     }
     return multiplier;
   }

   private double getTimeMultiplier(String unit) {
     return TIME_MULTIPLIERS.get(unit.toUpperCase());
   }
 
   @Override
   public void destroy() {
     // No cleanup needed.
   }
 
   @Override
   public Mutation lineage() {
     return Mutation.builder()
       .readable("Aggregates statistics for size and time columns")
       .build();
   }
 }

 
