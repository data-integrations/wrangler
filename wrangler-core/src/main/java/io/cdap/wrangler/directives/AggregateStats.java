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

 package io.cdap.wrangler.directives;

 import io.cdap.wrangler.api.Arguments;
 import io.cdap.wrangler.api.Directive;
 import io.cdap.wrangler.api.DirectiveExecutionException;
 import io.cdap.wrangler.api.DirectiveParseException;
 import io.cdap.wrangler.api.ExecutorContext;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.annotations.PublicEvolving;
 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.Text;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 
 import java.util.Collections;
 import java.util.List;
 
 /**
  * A directive that aggregates byte sizes and time durations from source columns
  * into target columns.
  */
 @PublicEvolving
 public class AggregateStats implements Directive {
   private String sizeColumn;
   private String timeColumn;
   private String targetSizeColumn;
   private String targetTimeColumn;
   private String sizeUnit = "MB";
   private String timeUnit = "s";
   private String aggregationType = "total";
 
   @Override
   public UsageDefinition define() {
     UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
     builder.define("size-column", TokenType.COLUMN_NAME);
     builder.define("time-column", TokenType.COLUMN_NAME);
     builder.define("target-size-column", TokenType.COLUMN_NAME);
     builder.define("target-time-column", TokenType.COLUMN_NAME);
     builder.define("size-unit", TokenType.TEXT, true);
     builder.define("time-unit", TokenType.TEXT, true);
     builder.define("aggregation-type", TokenType.TEXT, true);
     return builder.build();
   }
 
   @Override
   public void initialize(Arguments args) throws DirectiveParseException {
     this.sizeColumn = ((ColumnName) args.value("size-column")).value();
     this.timeColumn = ((ColumnName) args.value("time-column")).value();
     this.targetSizeColumn = ((ColumnName) args.value("target-size-column")).value();
     this.targetTimeColumn = ((ColumnName) args.value("target-time-column")).value();
     
     if (args.contains("size-unit")) {
       this.sizeUnit = ((Text) args.value("size-unit")).value();
     }
     if (args.contains("time-unit")) {
       this.timeUnit = ((Text) args.value("time-unit")).value();
     }
     if (args.contains("aggregation-type")) {
       this.aggregationType = ((Text) args.value("aggregation-type")).value();
     }
   }
 
   @Override
   public void destroy() {
     // No cleanup needed
   }
 
   @Override
   public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
     long totalBytes = 0;
     long totalNanos = 0;
     int count = 0;
 
     for (Row row : rows) {
       int sizeIdx = row.find(sizeColumn);
       int timeIdx = row.find(timeColumn);
 
       if (sizeIdx == -1) {
         throw new DirectiveExecutionException(
           String.format("Column '%s' not found in row", sizeColumn));
       }
       if (timeIdx == -1) {
         throw new DirectiveExecutionException(
           String.format("Column '%s' not found in row", timeColumn));
       }
 
       Object sizeValue = row.getValue(sizeIdx);
       Object timeValue = row.getValue(timeIdx);
 
       if (sizeValue instanceof ByteSize) {
         totalBytes += ((ByteSize) sizeValue).getBytes();
       } else if (sizeValue instanceof String) {
         totalBytes += new ByteSize((String) sizeValue).getBytes();
       } else {
         throw new DirectiveExecutionException(
           String.format("Column '%s' must contain byte size values", sizeColumn));
       }
 
       if (timeValue instanceof TimeDuration) {
         totalNanos += ((TimeDuration) timeValue).getNanoseconds();
       } else if (timeValue instanceof String) {
         totalNanos += new TimeDuration((String) timeValue).getNanoseconds();
       } else {
         throw new DirectiveExecutionException(
           String.format("Column '%s' must contain time duration values", timeColumn));
       }
 
       count++;
     }
 
     // Convert to target units
     double finalSize = convertBytes(totalBytes, sizeUnit);
     double finalTime = convertNanos(totalNanos, timeUnit);
 
     // Apply aggregation type
     if ("average".equalsIgnoreCase(aggregationType)) {
       finalSize = finalSize / count;
       finalTime = finalTime / count;
     }
 
     // Create result row
     Row result = new Row();
     result.add(targetSizeColumn, finalSize);
     result.add(targetTimeColumn, finalTime);
 
     return Collections.singletonList(result);
   }
 
   private double convertBytes(long bytes, String unit) throws DirectiveExecutionException {
     switch (unit.toUpperCase()) {
       case "B":
         return bytes;
       case "KB":
         return bytes / 1024.0;
       case "MB":
         return bytes / (1024.0 * 1024);
       case "GB":
         return bytes / (1024.0 * 1024 * 1024);
       case "TB":
         return bytes / (1024.0 * 1024 * 1024 * 1024);
       case "PB":
         return bytes / (1024.0 * 1024 * 1024 * 1024 * 1024);
       default:
         throw new DirectiveExecutionException("Invalid size unit: " + unit);
     }
   }
 
   private double convertNanos(long nanos, String unit) throws DirectiveExecutionException {
     switch (unit.toLowerCase()) {
       case "ns":
         return nanos;
       case "ms":
         return nanos / 1_000_000.0;
       case "s":
         return nanos / 1_000_000_000.0;
       case "m":
         return nanos / (60.0 * 1_000_000_000);
       case "h":
         return nanos / (60.0 * 60 * 1_000_000_000);
       case "d":
         return nanos / (24.0 * 60 * 60 * 1_000_000_000);
       default:
         throw new DirectiveExecutionException("Invalid time unit: " + unit);
     }
   }
 } 