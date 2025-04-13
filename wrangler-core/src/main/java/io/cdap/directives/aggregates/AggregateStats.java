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
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;

import java.util.List;

/**
 * Directive for aggregating byte sizes and time durations.
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateStats.NAME)
@Description("Aggregates byte sizes and time durations into total values")
public class AggregateStats implements Directive, Lineage {
  public static final String NAME = "aggregate-stats";
  private String sizeColumn;
  private String timeColumn;
  private String totalSizeColumn;
  private String totalTimeColumn;
  private long totalBytes;
  private long totalMilliseconds;
  private int rowCount;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("sizeColumn", TokenType.COLUMN_NAME);
    builder.define("timeColumn", TokenType.COLUMN_NAME);
    builder.define("totalSizeColumn", TokenType.COLUMN_NAME);
    builder.define("totalTimeColumn", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sizeColumn = ((ColumnName) args.value("sizeColumn")).value();
    this.timeColumn = ((ColumnName) args.value("timeColumn")).value();
    this.totalSizeColumn = ((ColumnName) args.value("totalSizeColumn")).value();
    this.totalTimeColumn = ((ColumnName) args.value("totalTimeColumn")).value();
    this.totalBytes = 0;
    this.totalMilliseconds = 0;
    this.rowCount = 0;
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int sizeIdx = row.find(sizeColumn);
      int timeIdx = row.find(timeColumn);
      
      if (sizeIdx != -1) {
        Object sizeObj = row.getValue(sizeIdx);
        if (sizeObj instanceof ByteSize) {
          ByteSize size = (ByteSize) sizeObj;
          totalBytes += size.getBytes();
        }
      }
      
      if (timeIdx != -1) {
        Object timeObj = row.getValue(timeIdx);
        if (timeObj instanceof TimeDuration) {
          TimeDuration duration = (TimeDuration) timeObj;
          totalMilliseconds += duration.getMilliseconds();
        }
      }
      rowCount++;
    }

    // Create a single row with aggregated results
    Row result = new Row();
    result.add(totalSizeColumn, totalBytes / (1024.0 * 1024.0)); // Convert to MB
    result.add(totalTimeColumn, totalMilliseconds / 1000.0); // Convert to seconds
    
    return List.of(result);
  }

  @Override
  public void destroy() {
    // No-op
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Aggregated byte sizes from '%s' and time durations from '%s' into total values in '%s' and '%s'", 
                sizeColumn, timeColumn, totalSizeColumn, totalTimeColumn)
      .relation(Many.of(sizeColumn, timeColumn), Many.of(totalSizeColumn, totalTimeColumn))
      .build();
  }
}
