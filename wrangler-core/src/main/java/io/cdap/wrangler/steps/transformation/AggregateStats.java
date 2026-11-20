/*
 * Copyright © 2025 Garv Tayal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package io.cdap.wrangler.steps.transformation;

 import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.Executor;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Description;
import io.cdap.wrangler.api.annotations.Name;
import io.cdap.wrangler.api.annotations.Plugin;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
 /**
 * Aggregate stats
 */

 @Plugin(type = Directive.TYPE)
 @Name("aggregate-stats")
 @Description("Computes aggregate statistics like count, sum, min, max, and average on a numeric column.")

 /**
 * Aggregate stats
 */
 public class AggregateStats implements Directive, Executor<List<Row>, List<Row>> {
     private String outputPrefix;
     private String column;
 
     public AggregateStats() {
         // Default constructor
     }
 
     public AggregateStats(String outputPrefix, String column) {
         this.outputPrefix = outputPrefix;
         this.column = column;
     }
 
     @Override
     public UsageDefinition define() {
         UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
         builder.define("output", TokenType.TEXT);
         builder.define("column", TokenType.COLUMN_NAME);
         return builder.build();
     }
 
     @Override
     public void initialize(Arguments arguments) {
         this.outputPrefix = arguments.value("output");
         this.column = arguments.value("column");
     }
 
     @Override
     public List<Row> execute(List<Row> rows, ExecutorContext context) {
         List<Double> values = new ArrayList<>();
         for (Row row : rows) {
           int idx = row.find(column);
           if (idx == -1) {
               throw new RuntimeException("Column not found: " + column);
           }
 
           Object val = row.getValue(idx);
           if (val instanceof Number) {
               values.add(((Number) val).doubleValue());
           } else {
               throw new RuntimeException("Non-numeric value encountered in column: " + column);
           }
       }
 
         long count = values.size();
         double sum = values.stream().mapToDouble(Double::doubleValue).sum();
         double min = values.stream().min(Comparator.naturalOrder()).orElse(0.0);
         double max = values.stream().max(Comparator.naturalOrder()).orElse(0.0);
         double avg = count > 0 ? sum / count : 0.0;
 
         Row result = new Row();
         result.add(outputPrefix + "_count", count);
         result.add(outputPrefix + "_sum", sum);
         result.add(outputPrefix + "_min", min);
         result.add(outputPrefix + "_max", max);
         result.add(outputPrefix + "_avg", avg);
 
         List<Row> output = new ArrayList<>();
         output.add(result);
         return output;
     }
 
     @Override
     public void destroy() {
         // no-op
     }
 }
