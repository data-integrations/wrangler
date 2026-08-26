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

package io.cdap.wrangler.steps.transformation;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A directive for aggregating byte size and time duration data.
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateStats.NAME)
@Description("Aggregates byte size and time duration data")
public class AggregateStats implements Directive {
  public static final String NAME = "aggregate-stats";

  private static final Logger LOG = LoggerFactory.getLogger(AggregateStats.class);

  private String sizeColumnName;
  private String timeColumnName;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("sizeColumn", TokenType.COLUMN_NAME);
    builder.define("timeColumn", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sizeColumnName = ((ColumnName) args.value("sizeColumn")).value();
    this.timeColumnName = ((ColumnName) args.value("timeColumn")).value();
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    // Use Map as TransientStore fallback
    @SuppressWarnings("unchecked")
    Map<String, Object> store = (Map<String, Object>) context.getTransientStore();

    long totalBytes = store.getOrDefault("totalBytes", 0L) instanceof Long ? (Long) store.get("totalBytes") : 0L;
    long totalNanos = store.getOrDefault("totalNanos", 0L) instanceof Long ? (Long) store.get("totalNanos") : 0L;
    int rowCount = store.getOrDefault("rowCount", 0) instanceof Integer ? (Integer) store.get("rowCount") : 0;

    for (Row row : rows) {
      // Process byte size column
      if (row.find(sizeColumnName) != -1) {
        Object sizeObj = row.getValue(sizeColumnName);
        long bytes = 0;

        if (sizeObj instanceof ByteSize) {
          bytes = ((ByteSize) sizeObj).getBytes();
        } else if (sizeObj instanceof String) {
          try {
            bytes = new ByteSize(sizeObj.toString()).getBytes();
          } catch (Exception e) {
            LOG.warn("Could not parse byte size: {}", sizeObj);
          }
        }

        totalBytes += bytes;
      }

      // Process time duration column
      if (row.find(timeColumnName) != -1) {
        Object timeObj = row.getValue(timeColumnName);
        long nanos = 0;

        if (timeObj instanceof TimeDuration) {
          nanos = ((TimeDuration) timeObj).getNanoseconds();
        } else if (timeObj instanceof String) {
          try {
            nanos = new TimeDuration(timeObj.toString()).getNanoseconds();
          } catch (Exception e) {
            LOG.warn("Could not parse time duration: {}", timeObj);
          }
        }

        totalNanos += nanos;
      }

      rowCount++;
    }

    // Store back the aggregated values
    store.put("totalBytes", totalBytes);
    store.put("totalNanos", totalNanos);
    store.put("rowCount", rowCount);

    return rows; // No summary row added; you can add it externally if needed
  }

  @Override
  public void destroy() {
    // Optional cleanup if needed
  }
}
