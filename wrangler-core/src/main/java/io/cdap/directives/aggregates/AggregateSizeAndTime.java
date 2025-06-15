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

import java.util.ArrayList;
import java.util.List;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

/**
 * A directive for aggregating byte sizes and time durations across rows.
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateSizeAndTime.NAME)
@Categories(categories = { "aggregates"})
@Description("Aggregates byte sizes and time durations across rows into target columns.")
public class AggregateSizeAndTime implements Directive {
  public static final String NAME = "aggregate-size-time";
  private String sourceSizeColumn;
  private String sourceTimeColumn;
  private String targetSizeColumn;
  private String targetTimeColumn;
  private String sizeUnit;
  private String timeUnit;
  private String aggregationType;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source-size", TokenType.COLUMN_NAME);
    builder.define("source-time", TokenType.COLUMN_NAME);
    builder.define("target-size", TokenType.COLUMN_NAME);
    builder.define("target-time", TokenType.COLUMN_NAME);
    builder.define("size-unit", TokenType.IDENTIFIER, true);
    builder.define("time-unit", TokenType.IDENTIFIER, true);
    builder.define("aggregation", TokenType.IDENTIFIER, true);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sourceSizeColumn = ((ColumnName) args.value("source-size")).value();
    this.sourceTimeColumn = ((ColumnName) args.value("source-time")).value();
    this.targetSizeColumn = ((ColumnName) args.value("target-size")).value();
    this.targetTimeColumn = ((ColumnName) args.value("target-time")).value();
    
    if (args.contains("size-unit")) {
      this.sizeUnit = ((Identifier) args.value("size-unit")).value();
    } else {
      this.sizeUnit = "B"; // Default to bytes
    }
    
    if (args.contains("time-unit")) {
      this.timeUnit = ((Identifier) args.value("time-unit")).value();
    } else {
      this.timeUnit = "ns"; // Default to nanoseconds
    }
    
    if (args.contains("aggregation")) {
      this.aggregationType = ((Identifier) args.value("aggregation")).value();
    } else {
      this.aggregationType = "total"; // Default to total
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    if (rows.isEmpty()) {
      return rows;
    }

    // Initialize aggregate values in transient store if not already present
    if (context != null) {
      if (!context.getTransientStore().getVariables().contains("total_size")) {
        context.getTransientStore().set(TransientVariableScope.GLOBAL, "total_size", 0L);
      }
      if (!context.getTransientStore().getVariables().contains("total_time")) {
        context.getTransientStore().set(TransientVariableScope.GLOBAL, "total_time", 0L);
      }
      if (!context.getTransientStore().getVariables().contains("row_count")) {
        context.getTransientStore().set(TransientVariableScope.GLOBAL, "row_count", 0L);
      }
    }

    // Process each row
    for (Row row : rows) {
      int sizeIdx = row.find(sourceSizeColumn);
      int timeIdx = row.find(sourceTimeColumn);
      
      if (sizeIdx == -1 || timeIdx == -1) {
        continue;
      }

      try {
        // Parse byte size and time duration values
        String sizeStr = String.valueOf(row.getValue(sizeIdx));
        String timeStr = String.valueOf(row.getValue(timeIdx));
        
        ByteSize byteSize = new ByteSize(sizeStr);
        TimeDuration timeDuration = new TimeDuration(timeStr);
        
        // Add to running totals
        if (context != null) {
          context.getTransientStore().increment(TransientVariableScope.GLOBAL, "total_size", 
              ((Number)byteSize.value()).longValue());
          context.getTransientStore().increment(TransientVariableScope.GLOBAL, "total_time",
              ((Number)timeDuration.value()).longValue());
          context.getTransientStore().increment(TransientVariableScope.GLOBAL, "row_count", 1L);
        }
      } catch (Exception e) {
        throw new DirectiveExecutionException(NAME, 
            String.format("Failed to parse size or time value: %s", e.getMessage()), e);
      }
    }

    // Create result row with aggregated values
    List<Row> results = new ArrayList<>();
    Row result = new Row();
    
    if (context != null) {
      long totalSize = context.getTransientStore().get("total_size");
      long totalTime = context.getTransientStore().get("total_time");
      long rowCount = context.getTransientStore().get("row_count");
      
      // Convert to requested units
      if (!sizeUnit.equals("B")) {
        totalSize = convertSize(totalSize, sizeUnit);
      }
      
      if (!timeUnit.equals("ns")) {
        totalTime = convertTime(totalTime, timeUnit);
      }
      
      // Calculate final value based on aggregation type
      if (aggregationType.equals("average")) {
        totalSize = rowCount > 0 ? totalSize / rowCount : 0;
        totalTime = rowCount > 0 ? totalTime / rowCount : 0;
      }
      
      result.add(targetSizeColumn, totalSize);
      result.add(targetTimeColumn, totalTime);
    }
    
    results.add(result);
    return results;
  }

  private long convertSize(long bytes, String unit) {
    switch (unit.toUpperCase()) {
      case "KB":
        return bytes / 1024;
      case "MB":
        return bytes / (1024 * 1024);
      case "GB":
        return bytes / (1024 * 1024 * 1024);
      case "TB":
        return bytes / (1024L * 1024 * 1024 * 1024);
      default:
        return bytes;
    }
  }

  private long convertTime(long nanoseconds, String unit) {
    switch (unit.toLowerCase()) {
      case "ms":
        return nanoseconds / 1_000_000;
      case "s":
        return nanoseconds / 1_000_000_000;
      case "m":
        return nanoseconds / (60L * 1_000_000_000);
      case "h":
        return nanoseconds / (3600L * 1_000_000_000);
      default:
        return nanoseconds;
    }
  }
} 