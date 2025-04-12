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
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ReportErrorAndProceed;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for aggregating byte size and time duration statistics.
 */
public class AggregateStats implements Directive {
  private String sizeColumn;
  private String timeColumn;
  private String totalSizeColumn;
  private String totalTimeColumn;
  private long totalBytes;
  private long totalMilliseconds;
  private int count;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
    builder.define("size_column", TokenType.COLUMN_NAME, "Column containing byte size values");
    builder.define("time_column", TokenType.COLUMN_NAME, "Column containing time duration values");
    builder.define("total_size_column", TokenType.COLUMN_NAME, "Column to store total size");
    builder.define("total_time_column", TokenType.COLUMN_NAME, "Column to store total time");
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    sizeColumn = ((ColumnName) args.value("size_column")).value();
    timeColumn = ((ColumnName) args.value("time_column")).value();
    totalSizeColumn = ((ColumnName) args.value("total_size_column")).value();
    totalTimeColumn = ((ColumnName) args.value("total_time_column")).value();
    totalBytes = 0;
    totalMilliseconds = 0;
    count = 0;
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) 
      throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed {
    for (Row row : rows) {
      Object sizeObj = row.getValue(sizeColumn);
      Object timeObj = row.getValue(timeColumn);

      if (sizeObj != null) {
        ByteSize size = new ByteSize(sizeObj.toString());
        totalBytes += size.getBytes();
      }

      if (timeObj != null) {
        TimeDuration time = new TimeDuration(timeObj.toString());
        totalMilliseconds += time.getMilliseconds();
      }

      count++;
    }

    // Create a single row with the aggregated values
    Row result = new Row();
    result.add(totalSizeColumn, totalBytes);
    result.add(totalTimeColumn, totalMilliseconds);

    List<Row> results = new ArrayList<>();
    results.add(result);
    return results;
  }

  @Override
  public void destroy() {
    // No cleanup needed
  }
} 