/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package io.cdap.directives;

 import io.cdap.wrangler.api.Arguments;
 import io.cdap.wrangler.api.Directive;
 import io.cdap.wrangler.api.ExecutorContext;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 
 import java.util.Collections;
 import java.util.List;
 
 /**
  * A custom directive that aggregates byte size and time duration columns.
  */
 public class AggregateStats implements Directive {
 
   private String sizeColumn;
   private String timeColumn;
   private String outputSizeColumn;
   private String outputTimeColumn;
 
   private long totalBytes = 0;
   private long totalMilliseconds = 0;
 
   @Override
public UsageDefinition define() {
  UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
  builder.define("sizeColumn", ColumnName.class);
  builder.define("timeColumn", ColumnName.class);
  builder.define("outputSizeColumn", ColumnName.class);
  builder.define("outputTimeColumn", ColumnName.class);
  return builder.build();
}

 
   @Override
   public void initialize(Arguments arguments) {
     this.sizeColumn = ((ColumnName) arguments.value("sizeColumn")).value();
     this.timeColumn = ((ColumnName) arguments.value("timeColumn")).value();
     this.outputSizeColumn = ((ColumnName) arguments.value("outputSizeColumn")).value();
     this.outputTimeColumn = ((ColumnName) arguments.value("outputTimeColumn")).value();
   }
 
   @Override
   public List<Row> execute(List<Row> rows, ExecutorContext context) {
     for (Row row : rows) {
       Object sizeValue = row.getValue(sizeColumn);
       Object timeValue = row.getValue(timeColumn);
 
       if (sizeValue instanceof String) {
         ByteSize byteSize = new ByteSize((String) sizeValue);
         totalBytes += byteSize.getBytes();
       }
 
       if (timeValue instanceof String) {
         TimeDuration timeDuration = new TimeDuration((String) timeValue);
         totalMilliseconds += timeDuration.getMilliseconds();
       }
     }
 
     double totalSizeInMB = totalBytes / (1024.0 * 1024);
     double totalTimeInSec = totalMilliseconds / 1000.0;
 
     Row result = new Row();
     result.add(outputSizeColumn, totalSizeInMB);
     result.add(outputTimeColumn, totalTimeInSec);
 
     return Collections.singletonList(result);
   }
 
   @Override
   public void destroy() {
     // No cleanup required
   }
 }
 