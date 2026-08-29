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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package io.cdap.wrangler.directive.aggregate;

 import io.cdap.wrangler.api.Directive;
 import io.cdap.wrangler.api.DirectiveContext;
 import io.cdap.wrangler.api.DirectiveExecuteException;
 import io.cdap.wrangler.api.Executor;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.Arguments;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import io.cdap.wrangler.api.annotations.Description;
 import io.cdap.wrangler.api.annotations.Example;
 import io.cdap.wrangler.api.annotations.Name;
 
 import java.util.Collections;
 import java.util.List;
 
 /**
  * Aggregates total bytes and duration across rows.
  */
 @Name("aggregate-stats")
 @Description("Aggregates total bytes and duration across rows and outputs size in MB and time in seconds.")
 @Example("aggregate-stats :bytes :duration :total_mb :total_sec")
 public class AggregateStats implements Directive {
   private String sizeCol;
   private String timeCol;
   private String outputSizeCol;
   private String outputTimeCol;
 
   @Override
   public UsageDefinition define() {
     return UsageDefinition.builder("aggregate-stats")
       .define("sizeCol", TokenType.COLUMN_NAME)
       .define("timeCol", TokenType.COLUMN_NAME)
       .define("outputSizeCol", TokenType.COLUMN_NAME)
       .define("outputTimeCol", TokenType.COLUMN_NAME)
       .build();
   }
 
   @Override
   public void initialize(Arguments args) {
     sizeCol = ((ColumnName) args.value("sizeCol")).value();
     timeCol = ((ColumnName) args.value("timeCol")).value();
     outputSizeCol = ((ColumnName) args.value("outputSizeCol")).value();
     outputTimeCol = ((ColumnName) args.value("outputTimeCol")).value();
   }
 
   @Override
   public List<Row> execute(List<Row> rows, ExecutorContext ctx) throws DirectiveExecuteException {
     long totalBytes = 0;
     long totalMillis = 0;
 
     for (Row row : rows) {
       try {
         String sizeVal = row.getValue(sizeCol).toString();
         String timeVal = row.getValue(timeCol).toString();
 
         totalBytes += new ByteSize(sizeVal).getBytes();
         totalMillis += new TimeDuration(timeVal).getMillis();
       } catch (Exception e) {
         throw new DirectiveExecuteException("Failed to parse row values: " + e.getMessage(), e);
       }
     }
 
     Row result = new Row();
     result.add(outputSizeCol, totalBytes / (1024.0 * 1024));  // Output in MB
     result.add(outputTimeCol, totalMillis / 1000.0);          // Output in seconds
 
     return Collections.singletonList(result);
   }
 
   @Override
   public void destroy() {
     // Optional cleanup
   }
 }
 