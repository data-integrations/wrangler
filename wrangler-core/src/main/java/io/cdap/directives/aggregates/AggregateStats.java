/*
 * Copyright 2025 Cask Data, Inc.
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

package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.Executor;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregates byte size and time duration columns, outputs total or average in desired unit.
 */
public class AggregateStats implements Executor {

  private String sizeCol;
  private String timeCol;
  private String outputSizeCol;
  private String outputTimeCol;
  private String unit;
  private String aggType;

  @Override
  public void initialize(Arguments arguments) {
    // Initialize the arguments from the context
    this.sizeCol = arguments.value("sizeCol");
    this.timeCol = arguments.value("timeCol");
    this.outputSizeCol = arguments.value("outputSizeCol");
    this.outputTimeCol = arguments.value("outputTimeCol");
    this.unit = arguments.value("unit") != null ? arguments.value("unit") : "B";
    this.aggType = arguments.value("aggType") != null ? arguments.value("aggType") : "total";
  }

  /**
   * Executes the aggregation on the rows, calculating the total or average size and time duration.
   */
  @Override
  public List<Row> execute(Object input, ExecutorContext context) {
    List<Row> rows = (List<Row>) input;  // Cast Object to List<Row>
    TransientStore store = context.getTransientStore();

    // Retrieve previously stored values or initialize them
    long totalBytes = getLong(store, "agg_total_bytes", 0L);
    long totalTimeNs = getLong(store, "agg_total_time_ns", 0L);
    int rowCount = getInt(store, "agg_row_count", 0);

    // Iterate through the rows to calculate the totals
    for (Row row : rows) {
      ByteSize byteSize = (ByteSize) row.getValue(sizeCol);
      TimeDuration timeDuration = (TimeDuration) row.getValue(timeCol);

      totalBytes += byteSize.getBytes();
      totalTimeNs += timeDuration.getMilliseconds() * 1_000_000;  // Convert to nanoseconds
      rowCount++;
    }

    // Store the updated totals in the TransientStore
    store.set(TransientVariableScope.GLOBAL, "agg_total_bytes", totalBytes);
    store.set(TransientVariableScope.GLOBAL, "agg_total_time_ns", totalTimeNs);
    store.set(TransientVariableScope.GLOBAL, "agg_row_count", rowCount);

    // Convert the totals to the desired units
    long outputBytes = convertBytes(totalBytes, unit);
    long outputTime = convertTime(totalTimeNs, unit);

    // If aggregation type is "average", divide by the row count
    if ("average".equalsIgnoreCase(aggType) && rowCount > 0) {
      outputBytes /= rowCount;
      outputTime /= rowCount;
    }

    // Prepare the result row
    List<Row> result = new ArrayList<>();
    Row resultRow = new Row();
    resultRow.add(outputSizeCol, outputBytes);
    resultRow.add(outputTimeCol, outputTime);
    result.add(resultRow);

    return result;
  }

  @Override
  public void destroy() {
    // No-op: Cleanup if necessary (e.g., release resources)
  }

  /**
   * Convert bytes to the desired unit (KB, MB, GB).
   */
  private long convertBytes(long bytes, String unit) {
    switch (unit.toUpperCase()) {
      case "KB": return bytes / 1024;
      case "MB": return bytes / (1024 * 1024);
      case "GB": return bytes / (1024 * 1024 * 1024);
      default: return bytes;
    }
  }

  /**
   * Convert time in nanoseconds to the desired unit (microseconds, milliseconds, etc.).
   */
  private long convertTime(long nanoseconds, String unit) {
    switch (unit.toLowerCase()) {
      case "microseconds": return nanoseconds / 1_000;
      case "milliseconds": return nanoseconds / 1_000_000;
      case "seconds": return nanoseconds / 1_000_000_000;
      case "minutes": return nanoseconds / (60L * 1_000_000_000);
      default: return nanoseconds;
    }
  }

  /**
   * Get a long value from the TransientStore, or return the default value if the key is not found.
   */
  private long getLong(TransientStore store, String key, long defaultVal) {
    Object val = store.get(key);
    return val instanceof Number ? ((Number) val).longValue() : defaultVal;
  }

  /**
   * Get an int value from the TransientStore, or return the default value if the key is not found.
   */
  private int getInt(TransientStore store, String key, int defaultVal) {
    Object val = store.get(key);
    return val instanceof Number ? ((Number) val).intValue() : defaultVal;
  }
}
