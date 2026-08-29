/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveDefinition;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.EntityCountMetric;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TokenType;
import io.cdap.wrangler.api.annotations.Category;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenList;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.TimeDuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A directive for aggregating statistics on byte size and time duration values.
 * This directive can calculate sum, average, min, and max for these special types.
 */
@PublicEvolving
@Category(description = "Aggregates statistics on byte size and time duration values")
public class AggregateStats implements Directive {
  public static final String NAME = "aggregate-stats";
  private String column;
  private String operation;
  private String outputColumn;

  @Override
  public DirectiveDefinition define() {
    return DirectiveDefinition.builder(NAME)
      .setDescription("Aggregates statistics on byte size and time duration values")
      .setCategory("transformation")
      .build();
  }

  @Override
  public void initialize(ExecutorContext context) throws DirectiveParseException {
    // No initialization needed
  }

  @Override
  public List<Row> execute(List<Row> rows, DirectiveContext context) throws DirectiveExecutionException {
    if (rows == null || rows.isEmpty()) {
      return rows;
    }

    // Validate input
    if (column == null || column.isEmpty()) {
      throw new DirectiveExecutionException(NAME, "Column name is required");
    }

    if (operation == null || operation.isEmpty()) {
      throw new DirectiveExecutionException(NAME, "Operation is required");
    }

    if (outputColumn == null || outputColumn.isEmpty()) {
      outputColumn = column + "_" + operation;
    }

    // Process the rows
    List<Row> result = new ArrayList<>();
    Object aggregatedValue = null;

    // Determine the type of values we're dealing with
    TokenType valueType = null;
    for (Row row : rows) {
      Object value = row.getValue(column);
      if (value != null) {
        if (value instanceof ByteSize) {
          valueType = TokenType.BYTE_SIZE;
          break;
        } else if (value instanceof TimeDuration) {
          valueType = TokenType.TIME_DURATION;
          break;
        }
      }
    }

    if (valueType == null) {
      throw new DirectiveExecutionException(NAME, 
          "Column '" + column + "' does not contain byte size or time duration values");
    }

    // Perform the aggregation based on the operation and value type
    switch (operation.toLowerCase()) {
      case "sum":
        aggregatedValue = calculateSum(rows, column, valueType);
        break;
      case "avg":
        aggregatedValue = calculateAverage(rows, column, valueType);
        break;
      case "min":
        aggregatedValue = calculateMin(rows, column, valueType);
        break;
      case "max":
        aggregatedValue = calculateMax(rows, column, valueType);
        break;
      default:
        throw new DirectiveExecutionException(NAME, 
            "Unsupported operation: " + operation + ". Supported operations are: sum, avg, min, max");
    }

    // Add the aggregated value to each row
    for (Row row : rows) {
      Row newRow = new Row(row);
      newRow.add(outputColumn, aggregatedValue);
      result.add(newRow);
    }

    return result;
  }

  private Object calculateSum(List<Row> rows, String column, TokenType valueType) {
    if (valueType == TokenType.BYTE_SIZE) {
      long sum = 0;
      for (Row row : rows) {
        Object value = row.getValue(column);
        if (value instanceof ByteSize) {
          sum += ((ByteSize) value).toBytes();
        }
      }
      return new ByteSize(sum, "B", sum + "B");
    } else if (valueType == TokenType.TIME_DURATION) {
      long sum = 0;
      for (Row row : rows) {
        Object value = row.getValue(column);
        if (value instanceof TimeDuration) {
          sum += ((TimeDuration) value).toMillis();
        }
      }
      return new TimeDuration(sum, "ms", sum + "ms");
    }
    return null;
  }

  private Object calculateAverage(List<Row> rows, String column, TokenType valueType) {
    if (valueType == TokenType.BYTE_SIZE) {
      long sum = 0;
      int count = 0;
      for (Row row : rows) {
        Object value = row.getValue(column);
        if (value instanceof ByteSize) {
          sum += ((ByteSize) value).toBytes();
          count++;
        }
      }
      if (count == 0) {
        return new ByteSize(0, "B", "0B");
      }
      long avg = sum / count;
      return new ByteSize(avg, "B", avg + "B");
    } else if (valueType == TokenType.TIME_DURATION) {
      long sum = 0;
      int count = 0;
      for (Row row : rows) {
        Object value = row.getValue(column);
        if (value instanceof TimeDuration) {
          sum += ((TimeDuration) value).toMillis();
          count++;
        }
      }
      if (count == 0) {
        return new TimeDuration(0, "ms", "0ms");
      }
      long avg = sum / count;
      return new TimeDuration(avg, "ms", avg + "ms");
    }
    return null;
  }

  private Object calculateMin(List<Row> rows, String column, TokenType valueType) {
    if (valueType == TokenType.BYTE_SIZE) {
      long min = Long.MAX_VALUE;
      ByteSize minByteSize = null;
      for (Row row : rows) {
        Object value = row.getValue(column);
        if (value instanceof ByteSize) {
          ByteSize byteSize = (ByteSize) value;
          long bytes = byteSize.toBytes();
          if (bytes < min) {
            min = bytes;
            minByteSize = byteSize;
          }
        }
      }
      return minByteSize != null ? minByteSize : new ByteSize(0, "B", "0B");
    } else if (valueType == TokenType.TIME_DURATION) {
      long min = Long.MAX_VALUE;
      TimeDuration minTimeDuration = null;
      for (Row row : rows) {
        Object value = row.getValue(column);
        if (value instanceof TimeDuration) {
          TimeDuration timeDuration = (TimeDuration) value;
          long millis = timeDuration.toMillis();
          if (millis < min) {
            min = millis;
            minTimeDuration = timeDuration;
          }
        }
      }
      return minTimeDuration != null ? minTimeDuration : new TimeDuration(0, "ms", "0ms");
    }
    return null;
  }

  private Object calculateMax(List<Row> rows, String column, TokenType valueType) {
    if (valueType == TokenType.BYTE_SIZE) {
      long max = Long.MIN_VALUE;
      ByteSize maxByteSize = null;
      for (Row row : rows) {
        Object value = row.getValue(column);
        if (value instanceof ByteSize) {
          ByteSize byteSize = (ByteSize) value;
          long bytes = byteSize.toBytes();
          if (bytes > max) {
            max = bytes;
            maxByteSize = byteSize;
          }
        }
      }
      return maxByteSize != null ? maxByteSize : new ByteSize(0, "B", "0B");
    } else if (valueType == TokenType.TIME_DURATION) {
      long max = Long.MIN_VALUE;
      TimeDuration maxTimeDuration = null;
      for (Row row : rows) {
        Object value = row.getValue(column);
        if (value instanceof TimeDuration) {
          TimeDuration timeDuration = (TimeDuration) value;
          long millis = timeDuration.toMillis();
          if (millis > max) {
            max = millis;
            maxTimeDuration = timeDuration;
          }
        }
      }
      return maxTimeDuration != null ? maxTimeDuration : new TimeDuration(0, "ms", "0ms");
    }
    return null;
  }

  @Override
  public void destroy() {
    // No cleanup needed
  }

  @Override
  public List<EntityCountMetric> getCountMetrics() {
    return null;
  }

  @Override
  public void configure(Map<String, String> arguments) throws DirectiveParseException {
    column = arguments.get("column");
    operation = arguments.get("operation");
    outputColumn = arguments.get("output-column");
  }
} 