/*
  * Copyright © 2025 Cask Data, Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
  * use this file except in compliance with the License. You may obtain a copy of
  * the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  * License for the specific language governing permissions and limitations under
  * the License.
  */
 
  package io.cdap.wrangler.extension.directives.row;
 
  import io.cdap.wrangler.api.*;
  import io.cdap.wrangler.api.annotations.Directive;
  import io.cdap.wrangler.api.parser.*;
  import io.cdap.wrangler.api.row.Row;
  import io.cdap.wrangler.utils.UnitConverter; // ✅ Import your unit converter
  
  import java.util.Collections;
  import java.util.List;
  
  @Directive(name = "aggregate-stats", description = "Aggregate byte sizes and time durations")
  public class AggregateStats implements Directive {
  
    private String sizeCol;
    private String timeCol;
    private String targetSizeCol;
    private String targetTimeCol;
    private String sizeUnit;
    private String timeUnit;
    private String aggregationType;
  
    @Override
    public UsageDefinition define() {
      return UsageDefinition.builder()
        .define("sizeColumn", TokenType.COLUMN_NAME)
        .define("timeColumn", TokenType.COLUMN_NAME)
        .define("targetSizeColumn", TokenType.COLUMN_NAME)
        .define("targetTimeColumn", TokenType.COLUMN_NAME)
        .defineOptional("sizeUnit", TokenType.TEXT)
        .defineOptional("timeUnit", TokenType.TEXT)
        .defineOptional("aggregationType", TokenType.TEXT)
        .build();
    }
  
    @Override
    public void initialize(Arguments arguments) throws DirectiveParseException {
      this.sizeCol = arguments.value("sizeColumn");
      this.timeCol = arguments.value("timeColumn");
      this.targetSizeCol = arguments.value("targetSizeColumn");
      this.targetTimeCol = arguments.value("targetTimeColumn");
      this.sizeUnit = arguments.valueOrDefault("sizeUnit", "bytes");
      this.timeUnit = arguments.valueOrDefault("timeUnit", "nanoseconds");
      this.aggregationType = arguments.valueOrDefault("aggregationType", "total");
    }
  
    @Override
    public List<Row> execute(Row row, ExecutorContext context) throws DirectiveExecutionException {
      Store store = context.getStore("aggregate-stats");
  
      long sizeBytes = UnitConverter.parseByteSize(row.getValue(sizeCol).toString());
      long timeNanos = UnitConverter.parseDuration(row.getValue(timeCol).toString());
  
      store.set("totalBytes", store.getOrDefault("totalBytes", 0L) + sizeBytes);
      store.set("totalTimeNanos", store.getOrDefault("totalTimeNanos", 0L) + timeNanos);
      store.set("count", store.getOrDefault("count", 0L) + 1);
  
      return Collections.emptyList();
    }
  
    @Override
    public List<Row> terminate(ExecutorContext context) {
      Store store = context.getStore("aggregate-stats");
  
      long totalBytes = store.getOrDefault("totalBytes", 0L);
      long totalNanos = store.getOrDefault("totalTimeNanos", 0L);
      long count = store.getOrDefault("count", 1L);
  
      String formattedSize = UnitConverter.formatByteSize(totalBytes, sizeUnit);
      String formattedTime = aggregationType.equalsIgnoreCase("average")
          ? UnitConverter.formatDuration(totalNanos / count, timeUnit)
          : UnitConverter.formatDuration(totalNanos, timeUnit);
  
      Row result = new Row();
      result.add(targetSizeCol, formattedSize);
      result.add(targetTimeCol, formattedTime);
  
      return Collections.singletonList(result);
    }
  }