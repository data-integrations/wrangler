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

 package io.cdap.wrangler.plugin.directives;

 import io.cdap.wrangler.api.*;
 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 
 import java.util.List;
 import java.util.Collections;  // Added this import
 
 public class AggregateStats implements Directive {
     private String sizeCol;
     private String timeCol;
     private String totalSizeCol;
     private String totalTimeCol;
 
     @Override
     public UsageDefinition define() {
         UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
         builder.define("sizeCol", TokenType.COLUMN_NAME);
         builder.define("timeCol", TokenType.COLUMN_NAME);
         builder.define("totalSizeCol", TokenType.COLUMN_NAME);
         builder.define("totalTimeCol", TokenType.COLUMN_NAME);
         return builder.build();
     }
 
     @Override
     public void initialize(Arguments args) {
         ColumnName sizeColName = (ColumnName) args.value("sizeCol");
         ColumnName timeColName = (ColumnName) args.value("timeCol");
         ColumnName totalSizeColName = (ColumnName) args.value("totalSizeCol");
         ColumnName totalTimeColName = (ColumnName) args.value("totalTimeCol");
         
         this.sizeCol = sizeColName.value();
         this.timeCol = timeColName.value();
         this.totalSizeCol = totalSizeColName.value();
         this.totalTimeCol = totalTimeColName.value();
     }
 
     @Override
     public List<Row> execute(List<Row> rows, ExecutorContext ctx) {
         long totalBytes = 0;
         long totalMillis = 0;
 
         for (Row row : rows) {
             Object sizeVal = row.getValue(sizeCol);
             Object timeVal = row.getValue(timeCol);
 
             if (sizeVal != null) {
                 try {
                     totalBytes += new ByteSize(sizeVal.toString()).getBytes();
                 } catch (Exception e) {
                     throw new RuntimeException("Invalid size format: " + sizeVal);
                 }
             }
 
             if (timeVal != null) {
                 try {
                     totalMillis += new TimeDuration(timeVal.toString()).getMilliseconds();
                 } catch (Exception e) {
                     throw new RuntimeException("Invalid time format: " + timeVal);
                 }
             }
         }
 
         double sizeMB = totalBytes / (1024.0 * 1024.0);
         double timeSec = totalMillis / 1000.0;
 
         Row resultRow = new Row();
         resultRow.add(totalSizeCol, Double.valueOf(sizeMB));
         resultRow.add(totalTimeCol, Double.valueOf(timeSec));
 
         return Collections.singletonList(resultRow);
     }
 
     @Override
     public void destroy() {
         // no-op
     }
 }