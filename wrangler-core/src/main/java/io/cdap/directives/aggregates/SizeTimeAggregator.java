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

 package io.cdap.directives.aggregates;

 import com.google.common.collect.ImmutableList;
 import io.cdap.cdap.api.annotation.Description;
 import io.cdap.cdap.api.annotation.Name;
 import io.cdap.cdap.api.annotation.Plugin;
 import io.cdap.wrangler.api.Arguments;
 import io.cdap.wrangler.api.Directive;
 import io.cdap.wrangler.api.DirectiveExecutionException;
 import io.cdap.wrangler.api.DirectiveParseException;
 import io.cdap.wrangler.api.ExecutorContext;
 import io.cdap.wrangler.api.Optional;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.annotations.Categories;
 import io.cdap.wrangler.api.lineage.Lineage;
 import io.cdap.wrangler.api.lineage.Mutation;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.Text;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 import io.cdap.wrangler.expression.EL;
 import io.cdap.wrangler.expression.ELContext;
 import io.cdap.wrangler.expression.ELException;
 import io.cdap.wrangler.expression.ELResult;
 
 import java.util.List;
 import java.util.concurrent.atomic.AtomicLong;
 
 /**
  * A directive for aggregating size and time values across rows.
  */
 @Plugin(type = Directive.TYPE)
 @Name("size-time-aggregator")
 @Categories(categories = { "aggregates" })
 @Description("Aggregates size and time values across rows, with optional unit conversion")
 public class SizeTimeAggregator implements Directive, Lineage {
   public static final String NAME = "size-time-aggregator";
   private String sizeColumn;
   private String timeColumn;
   private String targetSizeColumn;
   private String targetTimeColumn;
   private String sizeUnit = "B";  // Default to bytes
   private String timeUnit = "ns"; // Default to nanoseconds
   private String aggregationType = "total"; // Default to total
 
   @Override
   public UsageDefinition define() {
     UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
     builder.define("size-column", TokenType.COLUMN_NAME);
     builder.define("time-column", TokenType.COLUMN_NAME);
     builder.define("target-size-column", TokenType.COLUMN_NAME);
     builder.define("target-time-column", TokenType.COLUMN_NAME);
     builder.define("size-unit", TokenType.TEXT, Optional.TRUE);
     builder.define("time-unit", TokenType.TEXT, Optional.TRUE);
     builder.define("aggregation-type", TokenType.TEXT, Optional.TRUE);
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
   public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
     AtomicLong totalBytes = new AtomicLong(0);
     AtomicLong totalNanos = new AtomicLong(0);
     AtomicLong count = new AtomicLong(0);
 
     // Accumulate totals
     rows.forEach(row -> {
       Object sizeValue = row.getValue(row.find(sizeColumn));
       Object timeValue = row.getValue(row.find(timeColumn));
 
       // Convert size to bytes
       if (sizeValue instanceof String) {
         String sizeStr = (String) sizeValue;
         if (sizeStr.endsWith("B")) {
           totalBytes.addAndGet(parseByteSize(sizeStr));
         } else {
           totalBytes.addAndGet(Long.parseLong(sizeStr));
         }
       } else if (sizeValue instanceof Number) {
         totalBytes.addAndGet(((Number) sizeValue).longValue());
       }
 
       // Convert time to nanoseconds
       if (timeValue instanceof String) {
         String timeStr = (String) timeValue;
         totalNanos.addAndGet(parseTimeDuration(timeStr));
       } else if (timeValue instanceof Number) {
         totalNanos.addAndGet(((Number) timeValue).longValue());
       }
 
       count.incrementAndGet();
     });
 
     // Convert to target units
     double finalSize = convertBytes(totalBytes.get(), sizeUnit);
     double finalTime = convertNanos(totalNanos.get(), timeUnit);
 
     // Apply aggregation type
     if ("average".equalsIgnoreCase(aggregationType)) {
       finalSize = finalSize / count.get();
       finalTime = finalTime / count.get();
     }
 
     // Return single row with results
     Row result = new Row();
     result.add(targetSizeColumn, finalSize);
     result.add(targetTimeColumn, finalTime);
     return ImmutableList.of(result);
   }
 
   private long parseByteSize(String size) {
     size = size.trim().toUpperCase();
     long multiplier = 1;
     if (size.endsWith("KB")) {
       multiplier = 1024;
       size = size.substring(0, size.length() - 2);
     } else if (size.endsWith("MB")) {
       multiplier = 1024 * 1024;
       size = size.substring(0, size.length() - 2);
     } else if (size.endsWith("GB")) {
       multiplier = 1024 * 1024 * 1024;
       size = size.substring(0, size.length() - 2);
     } else if (size.endsWith("TB")) {
       multiplier = 1024L * 1024 * 1024 * 1024;
       size = size.substring(0, size.length() - 2);
     } else if (size.endsWith("PB")) {
       multiplier = 1024L * 1024 * 1024 * 1024 * 1024;
       size = size.substring(0, size.length() - 2);
     } else if (size.endsWith("B")) {
       size = size.substring(0, size.length() - 1);
     }
     return Long.parseLong(size) * multiplier;
   }
 
   private long parseTimeDuration(String duration) {
     duration = duration.trim().toLowerCase();
     long multiplier = 1;
     if (duration.endsWith("ns")) {
       multiplier = 1;
       duration = duration.substring(0, duration.length() - 2);
     } else if (duration.endsWith("ms")) {
       multiplier = 1_000_000;
       duration = duration.substring(0, duration.length() - 2);
     } else if (duration.endsWith("s")) {
       multiplier = 1_000_000_000;
       duration = duration.substring(0, duration.length() - 1);
     } else if (duration.endsWith("m")) {
       multiplier = 60L * 1_000_000_000;
       duration = duration.substring(0, duration.length() - 1);
     } else if (duration.endsWith("h")) {
       multiplier = 60L * 60 * 1_000_000_000;
       duration = duration.substring(0, duration.length() - 1);
     } else if (duration.endsWith("d")) {
       multiplier = 24L * 60 * 60 * 1_000_000_000;
       duration = duration.substring(0, duration.length() - 1);
     }
     return Long.parseLong(duration) * multiplier;
   }
 
   private double convertBytes(long bytes, String unit) throws DirectiveExecutionException {
     switch (unit.toUpperCase()) {
       case "B": return bytes;
       case "KB": return bytes / 1024.0;
       case "MB": return bytes / (1024.0 * 1024);
       case "GB": return bytes / (1024.0 * 1024 * 1024);
       case "TB": return bytes / (1024.0 * 1024 * 1024 * 1024);
       case "PB": return bytes / (1024.0 * 1024 * 1024 * 1024 * 1024);
       default: throw new DirectiveExecutionException("Invalid size unit: " + unit);
     }
   }
 
   private double convertNanos(long nanos, String unit) throws DirectiveExecutionException {
     switch (unit.toLowerCase()) {
       case "ns": return nanos;
       case "ms": return nanos / 1_000_000.0;
       case "s": return nanos / 1_000_000_000.0;
       case "m": return nanos / (60.0 * 1_000_000_000);
       case "h": return nanos / (60.0 * 60 * 1_000_000_000);
       case "d": return nanos / (24.0 * 60 * 60 * 1_000_000_000);
       default: throw new DirectiveExecutionException("Invalid time unit: " + unit);
     }
   }
 
   @Override
   public void destroy() {
     // no-op
   }
 
   @Override
   public Mutation lineage() {
     return Mutation.builder()
       .readable("Aggregated size and time values from columns '%s' and '%s'", sizeColumn, timeColumn)
       .relation(sizeColumn, targetSizeColumn)
       .relation(timeColumn, targetTimeColumn)
       .build();
   }
 } 