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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.token.ByteSize;
import io.cdap.wrangler.api.parser.token.TimeDuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A directive that aggregates statistics from columns containing byte size and time duration values.
 */
@Plugin(type = Directive.TYPE)
@Name("aggregate-stats")
@Categories(categories = {"aggregate"})
@Description("Aggregate statistics for columns with byte size and time duration values.")
public class AggregateStats implements Directive, Lineage {
  public static final String NAME = "aggregate-stats";
  private String sourceColumn;
  private String targetColumn;
  private String type;
  
  // Byte size conversion constants
  private static final double BYTES_PER_KB = 1024.0;
  private static final double BYTES_PER_MB = 1024.0 * 1024.0;
  private static final double BYTES_PER_GB = 1024.0 * 1024.0 * 1024.0;
  
  // Time conversion constants
  private static final double NANOS_PER_MS = 1_000_000.0;
  private static final double NANOS_PER_SECOND = 1_000_000_000.0;
  private static final double NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;
  private static final double NANOS_PER_HOUR = 3600 * NANOS_PER_SECOND;
  
  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("target", TokenType.COLUMN_NAME);
    builder.define("type", TokenType.TEXT);
    return builder.build();
  }
  
  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sourceColumn = ((ColumnName) args.value("source")).value();
    this.targetColumn = ((ColumnName) args.value("target")).value();
    this.type = ((Text) args.value("type")).value();
    
    if (!this.type.equals("byte") && !this.type.equals("time")) {
      throw new DirectiveParseException(
        String.format("Type should be either 'byte' or 'time', but got '%s'", this.type));
    }
  }
  
  @Override
  public void destroy() {
    // no-op
  }
  
  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) 
    throws DirectiveExecutionException, ErrorRowException {
    if (rows.isEmpty()) {
      return rows;
    }
    
    try {
      if (this.type.equals("byte")) {
        return processBytes(rows);
      } else {
        return processTime(rows);
      }
    } catch (Exception e) {
      throw new DirectiveExecutionException(
        String.format("Failed to aggregate stats due to '%s'", e.getMessage()), e);
    }
  }
  
  private List<Row> processBytes(List<Row> rows) {
    DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
    Map<String, Long> unitCounts = new HashMap<>();
    
    // Collect statistics
    for (Row row : rows) {
      if (row.getValue(sourceColumn) == null) {
        continue;
      }
      
      Object value = row.getValue(sourceColumn);
      if (value instanceof String) {
        try {
          ByteSize byteSize = new ByteSize((String) value);
          double bytes = byteSize.getBytes();
          stats.accept(bytes);
          
          // Count occurrences of each unit
          String unit = byteSize.getUnit();
          unitCounts.put(unit, unitCounts.getOrDefault(unit, 0L) + 1);
        } catch (Exception e) {
          // Skip invalid byte size values
          continue;
        }
      }
    }
    
    // Create result map with proper double precision
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put("count", stats.getCount());
    resultMap.put("sum", stats.getSum());
    resultMap.put("min", stats.getMin() != Double.POSITIVE_INFINITY ? stats.getMin() : 0.0);
    resultMap.put("max", stats.getMax() != Double.NEGATIVE_INFINITY ? stats.getMax() : 0.0);
    resultMap.put("avg", stats.getCount() > 0 ? stats.getAverage() : 0.0);
    resultMap.put("units", unitCounts);
    
    // Add human-readable values in different units with proper precision
    resultMap.put("sum_kb", stats.getSum() / BYTES_PER_KB);
    resultMap.put("sum_mb", stats.getSum() / BYTES_PER_MB);
    resultMap.put("sum_gb", stats.getSum() / BYTES_PER_GB);
    
    // Create a single result row
    Row resultRow = new Row();
    resultRow.add(targetColumn, resultMap);
    
    // Return a list containing only the single result row
    return Collections.singletonList(resultRow);
  }
  
  private List<Row> processTime(List<Row> rows) {
    DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
    Map<String, Long> unitCounts = new HashMap<>();
    
    // Collect statistics
    for (Row row : rows) {
      if (row.getValue(sourceColumn) == null) {
        continue;
      }
      
      Object value = row.getValue(sourceColumn);
      if (value instanceof String) {
        try {
          TimeDuration duration = new TimeDuration((String) value);
          double nanos = duration.getNanos();
          stats.accept(nanos);
          
          // Count occurrences of each unit
          String unit = duration.getUnit();
          unitCounts.put(unit, unitCounts.getOrDefault(unit, 0L) + 1);
        } catch (Exception e) {
          // Skip invalid time duration values
          continue;
        }
      }
    }
    
    // Create result map with proper double precision
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put("count", stats.getCount());
    resultMap.put("sum_nanos", stats.getSum());
    resultMap.put("min_nanos", stats.getMin() != Double.POSITIVE_INFINITY ? stats.getMin() : 0.0);
    resultMap.put("max_nanos", stats.getMax() != Double.NEGATIVE_INFINITY ? stats.getMax() : 0.0);
    resultMap.put("avg_nanos", stats.getCount() > 0 ? stats.getAverage() : 0.0);
    resultMap.put("units", unitCounts);
    
    // Add human-readable values in different units with proper precision
    double sumNanos = stats.getSum();
    resultMap.put("sum_ms", sumNanos / NANOS_PER_MS);
    resultMap.put("sum_s", sumNanos / NANOS_PER_SECOND);
    resultMap.put("sum_m", sumNanos / NANOS_PER_MINUTE);
    resultMap.put("sum_h", sumNanos / NANOS_PER_HOUR);
    
    // Create a single result row
    Row resultRow = new Row();
    resultRow.add(targetColumn, resultMap);
    
    // Return a list containing only the single result row
    return Collections.singletonList(resultRow);
  }
  
  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Aggregated statistics from column '%s' into column '%s'", sourceColumn, targetColumn)
      .relation(sourceColumn, targetColumn)
      .build();
  }
}
