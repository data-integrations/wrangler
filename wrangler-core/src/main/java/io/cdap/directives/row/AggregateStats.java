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

 package io.cdap.directives.row;

 import io.cdap.cdap.api.annotation.Description;
 import io.cdap.cdap.api.annotation.Name;
 import io.cdap.cdap.api.annotation.Plugin;
 import io.cdap.wrangler.api.*;
 import io.cdap.wrangler.api.annotations.Categories;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.Identifier;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.TimeDuration;
 
 import java.util.Collections;
 import java.util.List;
 import java.util.concurrent.TimeUnit;
 
 @Plugin(type = Directive.TYPE)
 @Name("aggregate-stats")
 @Categories(categories = { "row" })
 @Description("Aggregates BYTE_SIZE and TIME_DURATION columns and outputs a single row with stats")
 public class AggregateStats implements Directive {
   private static final String STORE_KEY = "aggregate-stats-accumulator";
 
   private String sizeColumn;
   private String durationColumn;
   private String outputSizeColumn;
   private String outputDurationColumn;
 
   private long totalBytes = 0;
   private long totalDurationSeconds = 0;
 
   @Override
   public UsageDefinition define() {
     UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
     builder.define("sizeColumn", TokenType.COLUMN_NAME);
     builder.define("durationColumn", TokenType.COLUMN_NAME);
     builder.define("outputSizeColumn", TokenType.IDENTIFIER);
     builder.define("outputDurationColumn", TokenType.IDENTIFIER);
     return builder.build();
   }
   

   

 
   @Override
   public void initialize(Arguments args) throws DirectiveParseException {
     sizeColumn = ((ColumnName) args.value("sizeColumn")).value();
     durationColumn = ((ColumnName) args.value("durationColumn")).value();
     outputSizeColumn = ((Identifier) args.value("outputSizeColumn")).value();
     outputDurationColumn = ((Identifier) args.value("outputDurationColumn")).value();
   }
 
   @Override
   public void destroy() {
     // No-op
   }
 
   @Override
public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
  for (Row row : rows) {
    Object sizeVal = row.getValue(sizeColumn);
    Object durationVal = row.getValue(durationColumn);

    if (sizeVal != null && sizeVal instanceof String) {
      totalBytes += ByteSize.getBytes((String) sizeVal);
    }

    if (durationVal != null && durationVal instanceof String) {
      totalDurationSeconds += TimeDuration.getDuration((String) durationVal, TimeUnit.SECONDS);
    }
  }

  // Store the accumulated data in some persistent storage or context
  // If getStore is not available, consider using an alternative method

  Row result = new Row();
  result.add(outputSizeColumn, totalBytes);
  result.add(outputDurationColumn, totalDurationSeconds);
  return Collections.singletonList(result);
}

 }
 