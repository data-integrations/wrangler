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

 package io.cdap.wrangler.parser.directives;

 
 import io.cdap.cdap.api.annotation.Description;
 import io.cdap.cdap.api.annotation.Name;
 import io.cdap.cdap.api.annotation.Plugin;
 import io.cdap.wrangler.api.Arguments;
 import io.cdap.wrangler.api.Directive;
 import io.cdap.wrangler.api.DirectiveParseException;
 import io.cdap.wrangler.api.ExecutorContext;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;

 
 import java.util.ArrayList;
 import java.util.List;
 
 
 /**
  * Directive to aggregate byte sizes and time durations across rows.
  */

@Plugin(type = "transform")
@Name("aggregate-stats")
@Description("Aggregates byte sizes and time durations across rows.")

 public class AggregateStatsDirective implements Directive {
 
     // Instance variables to store the column names and any additional configuration.
     private String srcByteCol;
     private String srcTimeCol;
     private String targetByteCol;
     private String targetTimeCol;
 
     @Override
     public UsageDefinition define() {
         // Create a builder for "aggregate-stats" and register expected arguments.
         // Since UsageDefinition.Builder does not support setDescription, we only define the required tokens.
         UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
 
         // For column names we typically use the TokenType.COLUMN_NAME.
         builder.define("srcByteCol", TokenType.COLUMN_NAME);
         builder.define("srcTimeCol", TokenType.COLUMN_NAME);
         builder.define("targetByteCol", TokenType.COLUMN_NAME);
         builder.define("targetTimeCol", TokenType.COLUMN_NAME);
 
         return builder.build();
     }
 
     /**
      * Initializes the directive with the provided arguments.
      *
      * @param args The arguments passed to the directive.
      * @throws DirectiveParseException if there are insufficient arguments.
      */
     @Override
     public void initialize(Arguments args) throws DirectiveParseException {
         // Validate that at least 4 arguments are provided.
         if (args.size() < 4) {
             throw new DirectiveParseException(
                 "Insufficient arguments for aggregate-stats directive. Required: 4, but found: " + args.size()
             );
         }
 
         // Extract the column names from the arguments.
         srcByteCol = args.value("srcByteCol").toString();
         srcTimeCol = args.value("srcTimeCol").toString();
         targetByteCol = args.value("targetByteCol").toString();
         targetTimeCol = args.value("targetTimeCol").toString();
     }
 
     /**
      * Executes the aggregation logic over a list of rows.
      *
      * @param rows The input rows.
      * @param context The execution context.
      * @return A list containing a single new aggregated row.
      * @throws DirectiveParseException if processing fails due to unexpected content.
      */
     @Override
     public List<Row> execute(List<Row> rows, ExecutorContext context) {
         long totalBytes = 0;
         long totalMilliseconds = 0;
 
         try {
             // Iterate over each row.
             for (Row row : rows) {
                 // Retrieve and process the byte size value.
                 Object byteVal = row.getValue(srcByteCol);
                 if (!(byteVal instanceof ByteSize)) {
                     throw new DirectiveParseException("Expected a ByteSize token in column " + srcByteCol);
                 }
                 ByteSize byteToken = (ByteSize) byteVal;
                 totalBytes += byteToken.getBytes();
 
                 // Retrieve and process the time duration value.
                 Object timeVal = row.getValue(srcTimeCol);
                 if (!(timeVal instanceof TimeDuration)) {
                     throw new DirectiveParseException("Expected a TimeDuration token in column " + srcTimeCol);
                 }
                 TimeDuration timeToken = (TimeDuration) timeVal;
                 totalMilliseconds += timeToken.getMilliseconds();
             }
 
             // Convert totals:
             double totalSizeMB = totalBytes / (1024.0 * 1024.0);
             double totalTimeSec = totalMilliseconds / 1000.0;
 
             // Create a new row to hold the aggregated results.
             Row outputRow = new Row();
             outputRow.add(targetByteCol, totalSizeMB);
             outputRow.add(targetTimeCol, totalTimeSec);
 
             // Return a list containing the aggregated row.
             List<Row> result = new ArrayList<>();
             result.add(outputRow);
             return result;
 
         } catch (DirectiveParseException e) {
             // Handle the exception by logging or rethrowing as a runtime exception.
             throw new RuntimeException("Error executing AggregateStatsDirective: " + e.getMessage(), e);
         }
     }
 
     @Override
     public void destroy() {
         // Any necessary cleanup logic goes here.
     }
}
