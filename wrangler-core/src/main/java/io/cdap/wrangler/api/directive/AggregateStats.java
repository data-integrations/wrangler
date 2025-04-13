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

package io.cdap.wrangler.api.directive;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for aggregating byte sizes and time durations.
 *
 * This directive takes source columns containing byte sizes and time durations,
 * and produces target columns with aggregated values in specified units.
 *
 * Example usage:
 * aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec
 */
public class AggregateStats implements Directive {
  private String sizeColumn;
  private String timeColumn;
  private String totalSizeColumn;
  private String totalTimeColumn;
  private String sizeUnit = "MB";
  private String timeUnit = "s";
  private String aggregationType = "total";

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
    builder.define("size_column", TokenType.COLUMN_NAME);
    builder.define("time_column", TokenType.COLUMN_NAME);
    builder.define("total_size_column", TokenType.COLUMN_NAME);
    builder.define("total_time_column", TokenType.COLUMN_NAME);
    builder.define("size_unit", TokenType.TEXT, true);
    builder.define("time_unit", TokenType.TEXT, true);
    builder.define("aggregation_type", TokenType.TEXT, true);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sizeColumn = ((ColumnName) args.value("size_column")).value();
    this.timeColumn = ((ColumnName) args.value("time_column")).value();
    this.totalSizeColumn = ((ColumnName) args.value("total_size_column")).value();
    this.totalTimeColumn = ((ColumnName) args.value("total_time_column")).value();

    if (args.contains("size_unit")) {
      this.sizeUnit = ((Text) args.value("size_unit")).value();
    }
    if (args.contains("time_unit")) {
      this.timeUnit = ((Text) args.value("time_unit")).value();
    }
    if (args.contains("aggregation_type")) {
      this.aggregationType = ((Text) args.value("aggregation_type")).value();
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, DirectiveContext context) throws DirectiveParseException {
    if (rows == null || rows.isEmpty()) {
      return rows;
    }

    long totalBytes = 0;
    long totalNanoseconds = 0;
    int count = 0;

    for (Row row : rows) {
      Object sizeValue = row.getValue(sizeColumn);
      Object timeValue = row.getValue(timeColumn);

      if (sizeValue != null) {
        ByteSize byteSize = new ByteSize(sizeValue.toString());
        totalBytes += byteSize.value();
      }

      if (timeValue != null) {
        TimeDuration timeDuration = new TimeDuration(timeValue.toString());
        totalNanoseconds += timeDuration.value();
      }

      count++;
    }

    // Create result row with aggregated values
    Row resultRow = new Row();
    
    // Convert total bytes to specified unit
    ByteSize totalSize = new ByteSize(totalBytes + "B");
    resultRow.add(totalSizeColumn, totalSize.getValue(sizeUnit));

    // Convert total nanoseconds to specified unit
    TimeDuration totalTime = new TimeDuration(totalNanoseconds + "ns");
    double timeValue = totalTime.getValue(timeUnit);
    
    // Apply aggregation type (total or average)
    if ("average".equalsIgnoreCase(aggregationType)) {
      timeValue = timeValue / count;
    }
    
    resultRow.add(totalTimeColumn, timeValue);

    List<Row> results = new ArrayList<>();
    results.add(resultRow);
    return results;
  }

  @Override
  public void destroy() {
    // No cleanup needed
  }
} 