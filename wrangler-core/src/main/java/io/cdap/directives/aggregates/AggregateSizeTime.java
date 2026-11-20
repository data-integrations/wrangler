/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
 import io.cdap.wrangler.api.ExecutorContext;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;
 
 /**
 * A directive that aggregates byte sizes and time durations from input rows into total or average values.
 *
 * <p>This directive operates on two input columns:
 * - One column containing byte sizes (e.g., "10MB", "1GB").
 * - Another column containing time durations (e.g., "500ms", "2s").
 *
 * <p>The directive supports both total and average aggregation types and allows specifying output units
 * for size (e.g., MB, GB) and time (e.g., seconds, minutes).
 *
 * <p>Example usage in a Wrangler recipe:
 * <pre>
 * {@code
 * aggregate-size-time :data_size :response_time total_size_mb total_time_sec 'MB' 'seconds' 'total';
 * }
 * </pre>
 *
 * @see Directive
 */

 @Plugin
 @Name("aggregateSizeTime")
 @Description("Aggregates byte size and time duration columns, returning total/average values.")
 @Categories(categories = { "aggregate" })
 public class AggregateSizeTime implements Directive {
 
   private String sizeColumn;
   private String timeColumn;
   private String targetSizeColumn;
   private String targetTimeColumn;
   private String outputSizeUnit;
   private String outputTimeUnit;
   private String aggregationType;
   private long totalSize;
   private long totalDuration;
   private int validRowCount;
   private Row resultRow;

   @Override
   public UsageDefinition define() {
    return UsageDefinition.builder("aggregate-stats")
        .define("sizeColumn", TokenType.COLUMN_NAME)
        .define("timeColumn", TokenType.COLUMN_NAME)
        .define("outputSizeColumn", TokenType.COLUMN_NAME)
        .define("outputTimeColumn", TokenType.COLUMN_NAME)
        .define("outputSizeUnit", TokenType.TEXT, true)
        .define("outputTimeUnit", TokenType.TEXT, true)
        .define("aggregationType", TokenType.TEXT, true)
        .build();
    }
 
   @Override
   public void initialize(Arguments args) {
     totalSize = 0;
     totalDuration = 0;
     validRowCount = 0;
     resultRow = new Row();

    // Correctly handle ColumnName tokens
    this.sizeColumn = ((ColumnName) args.value("sizeColumn")).value();
    this.timeColumn = ((ColumnName) args.value("timeColumn")).value();
    this.targetSizeColumn = ((ColumnName) args.value("outputSizeColumn")).value();
    this.targetTimeColumn = ((ColumnName) args.value("outputTimeColumn")).value();

    // Handle optional TEXT tokens with defaults
    this.outputSizeUnit = args.contains("outputSizeUnit") ? 
        ((Text) args.value("outputSizeUnit")).value().toLowerCase() : "MB";
    
    this.outputTimeUnit = args.contains("outputTimeUnit") ? 
        ((Text) args.value("outputTimeUnit")).value().toLowerCase() : "SECONDS";
    
    this.aggregationType = args.contains("aggregationType") ? 
        ((Text) args.value("aggregationType")).value().toLowerCase() : "total";
    }
 
   @Override
   public List<Row> execute(List<Row> rows, ExecutorContext context) {
   
     System.out.println("first time calling execute: totalSize: " + totalSize);

     for (Row row : rows) {
       Object sizeObj = row.getValue(sizeColumn);
       Object timeObj = row.getValue(timeColumn);
 
       System.out.println("Row: " + row);  
       System.out.println("sizeObj: " + sizeObj + ", timeObj: " + timeObj);
       System.out.println("orig totalSize: " + totalSize);
       System.out.println("orig totalDuration: " + totalDuration);

       boolean validRow = false;
 
       // Handle size value
       //if (sizeObj instanceof String) {
          try {
              totalSize += convertToBytes((String) sizeObj);  // Convert size to bytes and add to total
              System.out.println("new totalSize: " + totalSize);
              validRow = true;
          } catch (Exception e) {
              // If invalid, print the error and skip the size value
              System.out.println("Error in converting size: " + e.getMessage());
          }
       //}
 
       // Handle time value
       if (timeObj instanceof Number) {
         totalDuration += ((Number) timeObj).longValue();
         System.out.println("new totalDuration: " + totalDuration);
         validRow = true;
       } else if (timeObj instanceof String) {
         try {
           totalDuration += convertToNanos((String) timeObj);
           System.out.println("new totalDuration: " + totalDuration);
           validRow = true;
         } catch (Exception e) {
           // Skip invalid strings
         }
       }
 
       if (validRow) {
         validRowCount++;
       }
     }
 
     if ("average".equalsIgnoreCase(aggregationType) && validRowCount > 0) {
       totalSize = totalSize / validRowCount;
       totalDuration = totalDuration / validRowCount;
     }
 
     double finalSize = convertSize(totalSize, outputSizeUnit);
     double finalTime = convertTime(totalDuration, outputTimeUnit);
 
    //Row resultRow = new Row();
    if (resultRow.find(targetSizeColumn) != -1) {
      resultRow.remove(resultRow.find(targetSizeColumn));
      resultRow.remove(resultRow.find(targetTimeColumn));
    }
    resultRow.add(targetSizeColumn, finalSize);
    resultRow.add(targetTimeColumn, finalTime);
     
    List<Row> resultList = new ArrayList<>();
    resultList.add(resultRow);
    return resultList;

   }
 
   private double convertSize(long bytes, String unit) {
     switch (unit.toUpperCase()) {
       case "KB":
         return bytes / 1024.0;
       case "MB":
         return bytes / (1024.0 * 1024);
       case "GB":
         return bytes / (1024.0 * 1024 * 1024);
       default:
         return (double) bytes;
     }
   }
 
   private double convertTime(long nanos, String unit) {
    switch (unit.toLowerCase()) {
      case "s":
      case "sec":
      case "second":
      case "seconds":
        return nanos / 1_000_000_000.0;
      case "ms":
      case "millisecond":
      case "milliseconds":
        return nanos / 1_000_000.0;
      case "min":
      case "minute":
      case "minutes":
        return nanos / 60_000_000_000.0;
      default:
        return (double) nanos;
    }
   }
 
   private long convertToBytes(String input) {
    input = input.trim().toLowerCase();
    System.out.println("Inside convertToBytes, input = " + input);

    if (input.matches(".*\\d.*")) {  // has digits
      if (input.endsWith("kb")) {
        return (long) (Double.parseDouble(input.replaceAll("[^0-9.]", "")) * 1024);
      }
      if (input.endsWith("mb")) {
        return (long) (Double.parseDouble(input.replaceAll("[^0-9.]", "")) * 1024 * 1024);
      }
      if (input.endsWith("gb")) {
        return (long) (Double.parseDouble(input.replaceAll("[^0-9.]", "")) * 1024 * 1024 * 1024);
      }
    }
  
    // Fallback: try to parse raw number
    return Long.parseLong(input.replaceAll("[^0-9]", ""));
  }
  
 
   private long convertToNanos(String input) {
     input = input.toLowerCase().trim();
     if (input.endsWith("ms")) {
       return (long) (Double.parseDouble(input.replace("ms", "")) * 1_000_000);
     }
     if (input.endsWith("s")) {
       return (long) (Double.parseDouble(input.replace("s", "")) * 1_000_000_000);
     }
     if (input.endsWith("min")) {
       return (long) (Double.parseDouble(input.replace("min", "")) * 60_000_000_000L);
     }
     return Long.parseLong(input);
   }
 
   @Override
   public void destroy() {
     // No-op for now
   }
 }
