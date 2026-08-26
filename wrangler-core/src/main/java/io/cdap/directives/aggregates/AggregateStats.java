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

 import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.cdap.cdap.api.data.schema.Schema;
 import io.cdap.wrangler.api.*;
 import io.cdap.wrangler.api.annotations.Categories;
 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.Text;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 
 
 /**
  * Directive for aggregating statistics.
  */
 @Categories(categories = { "data-aggregation" })
 public class AggregateStats implements Directive {
   public static final String NAME = "aggregate-stats";
   private String byteCol;
   private String timeCol;
   private String outputSizeCol;
   private String outputTimeCol;
   private String aggregationType;
   private final List<Double> byteValues = new ArrayList<>();
   private final List<Double> timeValues = new ArrayList<>();
 
   private interface Aggregator {
     double aggregate(List<Double> values);
   }
 
   private static final Map<String, Aggregator> AGGREGATORS = Map.of(
     "total", values -> values.stream().mapToDouble(Double::doubleValue).sum(),
     "average", values -> values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0),
     "min", values -> values.stream().mapToDouble(Double::doubleValue).min().orElse(0.0),
     "max", values -> values.stream().mapToDouble(Double::doubleValue).max().orElse(0.0)
   );
 
   @Override
   public UsageDefinition define() {
     UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
     builder.define("byteCol", TokenType.COLUMN_NAME);
     builder.define("timeCol", TokenType.COLUMN_NAME);
     builder.define("outputSizeCol", TokenType.TEXT);
     builder.define("outputTimeCol", TokenType.TEXT);
     builder.define("aggregationType", TokenType.TEXT);
     return builder.build();
   }
 
   @Override
   public void initialize(Arguments args) throws DirectiveParseException {
     byteCol = ((ColumnName) args.value("byteCol")).value();
     timeCol = ((ColumnName) args.value("timeCol")).value();
     outputSizeCol = ((Text) args.value("outputSizeCol")).value();
     outputTimeCol = ((Text) args.value("outputTimeCol")).value();
     aggregationType = ((Text) args.value("aggregationType")).value().toLowerCase();
 
     if (!AGGREGATORS.containsKey(aggregationType)) {
       throw new DirectiveParseException("Unsupported aggregationType: " + aggregationType);
     }
   }
 
   @Override
   public List<Row> execute(List<Row> rows, ExecutorContext ctx) throws DirectiveExecutionException {
     try {
       for (Row row : rows) {
         if (row.find(byteCol) != -1 && row.find(timeCol) != -1) {
           String byteVal = row.getValue(byteCol).toString();
           String timeVal = row.getValue(timeCol).toString();
 
           ByteSize byteSize = new ByteSize(byteVal);
           TimeDuration duration = new TimeDuration(timeVal);
          
           byteValues.add(byteSize.getBytes() / (1024.0 * 1024.0)); // Convert to MB
           timeValues.add(duration.getNanoseconds() / 1_000_000_000.0); // Convert to seconds
         }
       }
 
       if (byteValues.isEmpty() || timeValues.isEmpty()) {
         return new ArrayList<>();
       }
 
       Aggregator aggregator = AGGREGATORS.get(aggregationType);
 
       List<Row> results = new ArrayList<>();
       Row result = new Row();
       result.add(outputSizeCol, aggregator.aggregate(byteValues));
       result.add(outputTimeCol, aggregator.aggregate(timeValues));
       results.add(result);
       return results;
 
     } catch (Exception e) {
       throw new DirectiveExecutionException(
         String.format("Error aggregating stats: %s", e.getMessage())
       );
     }
   }
 
   public Schema getOutputSchema(Schema inputSchema) {
     List<Schema.Field> fields = new ArrayList<>();
     fields.add(Schema.Field.of(outputSizeCol, Schema.of(Schema.Type.DOUBLE)));
     fields.add(Schema.Field.of(outputTimeCol, Schema.of(Schema.Type.DOUBLE)));
     return Schema.recordOf("aggregate-stats", fields);
   }
 
   @Override
   public void destroy() {
     byteValues.clear();
     timeValues.clear();
     byteCol = null;
     timeCol = null;
     outputSizeCol = null;
     outputTimeCol = null;
     aggregationType = null;
   }
 }
 