/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

 package io.cdap.wrangler.aggregate;

 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.Directive;
 import io.cdap.wrangler.api.ExecutorContext;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.Arguments;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 
 import java.util.List;
 import java.util.ArrayList;
 
 /**
  * AggregateStats directive to sum/average ByteSize and TimeDuration fields.
  */
 public class AggregateStats implements Directive {
   private String sizeColumn;
   private String timeColumn;
   private String totalSizeColumn;
   private String totalTimeColumn;
   private long totalBytes;
   private long totalMilliseconds;
 
   @Override
   public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
    builder.define("sizeColumn", TokenType.COLUMN_NAME);
    builder.define("timeColumn", TokenType.COLUMN_NAME);
    builder.define("totalSizeColumn", TokenType.COLUMN_NAME);
    builder.define("totalTimeColumn", TokenType.COLUMN_NAME);
    return builder.build();
  }

 
   @Override
   public void initialize(Arguments arguments) {
     this.sizeColumn = ((ColumnName) arguments.value("sizeColumn")).value();
     this.timeColumn = ((ColumnName) arguments.value("timeColumn")).value();
     this.totalSizeColumn = ((ColumnName) arguments.value("totalSizeColumn")).value();
     this.totalTimeColumn = ((ColumnName) arguments.value("totalTimeColumn")).value();
     this.totalBytes = 0;
     this.totalMilliseconds = 0;
   }
 
   @Override
   public List<Row> execute(List<Row> rows, ExecutorContext context) {
     for (Row row : rows) {
       Object sizeObj = row.getValue(sizeColumn);
       Object timeObj = row.getValue(timeColumn);
 
       if (sizeObj != null) {
         String sizeStr = sizeObj.toString();
         totalBytes += parseSize(sizeStr);
       }
 
       if (timeObj != null) {
         String timeStr = timeObj.toString();
         totalMilliseconds += parseTime(timeStr);
       }
     }
 
     List<Row> result = new ArrayList<>();
     Row output = new Row();
     output.add(totalSizeColumn, bytesToMegabytes(totalBytes));
     output.add(totalTimeColumn, millisecondsToSeconds(totalMilliseconds));
     result.add(output);
     return result;
   }
 
   @Override
   public void destroy() {
     // No cleanup needed
   }
 
   private long parseSize(String value) {
     value = value.trim().toUpperCase();
     if (value.endsWith("KB")) {
       return (long) (Double.parseDouble(value.replace("KB", "")) * 1024);
     } else if (value.endsWith("MB")) {
       return (long) (Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
     } else if (value.endsWith("GB")) {
       return (long) (Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
     } else if (value.endsWith("B")) {
       return (long) Double.parseDouble(value.replace("B", ""));
     } else {
       throw new IllegalArgumentException("Unknown byte size unit: " + value);
     }
   }
 
   private long parseTime(String value) {
     value = value.trim().toLowerCase();
     if (value.endsWith("ms")) {
       return (long) Double.parseDouble(value.replace("ms", ""));
     } else if (value.endsWith("s")) {
       return (long) (Double.parseDouble(value.replace("s", "")) * 1000);
     } else if (value.endsWith("m")) {
       return (long) (Double.parseDouble(value.replace("m", "")) * 60 * 1000);
     } else if (value.endsWith("h")) {
       return (long) (Double.parseDouble(value.replace("h", "")) * 60 * 60 * 1000);
     } else {
       throw new IllegalArgumentException("Unknown time duration unit: " + value);
     }
   }
 
   private double bytesToMegabytes(long bytes) {
     return bytes / (1024.0 * 1024.0);
   }
 
   private double millisecondsToSeconds(long millis) {
     return millis / 1000.0;
   }
 }
 