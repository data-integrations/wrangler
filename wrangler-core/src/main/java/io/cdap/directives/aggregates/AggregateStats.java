/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * Directive that aggregates byte size and time duration columns
 * and returns total/average in desired units.
 */
@PublicEvolving
public final class AggregateStats implements Directive {

   /** Number of bytes in a kilobyte. */
  private static final double KB = 1024.0;

  /** Number of bytes in a megabyte. */
  private static final double MB = KB * KB;

  /** Number of milliseconds in one second. */
  private static final double MS_IN_SEC = 1000.0;

  /** Input column containing byte size strings. */
  private String sizeColumn;

  /** Input column containing time duration strings. */
  private String timeColumn;

  /** Output column name for total size in megabytes. */
  private String targetSizeColumn;

  /** Output column name for total time in seconds. */
  private String targetTimeColumn;

  /** Total number of bytes aggregated from all rows. */
  private long totalBytes;

  /** Total number of milliseconds aggregated from all rows. */
  private long totalMillis;

  /** Total number of rows processed. */
  private int rowCount;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder =
    UsageDefinition.builder("aggregate-stats");
    builder.define("sizeColumn", TokenType.COLUMN_NAME);
    builder.define("timeColumn", TokenType.COLUMN_NAME);
    builder.define("targetSizeColumn", TokenType.COLUMN_NAME);
    builder.define("targetTimeColumn", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(final Arguments arguments) {
    sizeColumn = ((ColumnName) arguments.value("sizeColumn")).value();
    timeColumn = ((ColumnName) arguments.value("timeColumn")).value();
    targetSizeColumn = ((ColumnName) arguments.value(
                         "targetSizeColumn")).value();
    targetTimeColumn = ((ColumnName) arguments.value(
                         "targetTimeColumn")).value();
  }

  @Override
  public List<Row> execute(final List<Row> rows,
  final ExecutorContext context) {
    for (Row row : rows) {
      Object sizeValue = row.getValue(sizeColumn);
      Object timeValue = row.getValue(timeColumn);

      if (sizeValue instanceof String) {
        try {
          ByteSize byteSize = new ByteSize((String) sizeValue);
          totalBytes += byteSize.getBytes();
        } catch (Exception e) {
          System.err.println("Invalid byte size: " + sizeValue);
        }
      }

      if (timeValue instanceof String) {
        try {
          TimeDuration duration = new TimeDuration((String) timeValue);
          totalMillis += duration.getMilliseconds();
        } catch (Exception e) {
          System.err.println("Invalid time duration: " + timeValue);
        }
      }

      rowCount++;
    }

    List<Row> output = new ArrayList<>();
    Row aggregatedRow = new Row();

    double totalSizeMb = totalBytes / MB;
    double totalTimeSec = totalMillis / MS_IN_SEC;

    aggregatedRow.add(targetSizeColumn, totalSizeMb);
    aggregatedRow.add(targetTimeColumn, totalTimeSec);

    output.add(aggregatedRow);
    return output;
  }

  @Override
  public void destroy() {
    // No cleanup necessary
  }
}
