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
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A directive for aggregating statistics about the data.
 */
@Plugin(type = Directive.TYPE)
@Name("aggregate-stats")
@Categories(categories = {"aggregate"})
@Description("Aggregates statistics about the data including total bytes and time duration.")
public class AggregateStats implements Directive {
  public static final String NAME = "aggregate-stats";
  private static final String TOTAL_BYTES = "total_bytes";
  private static final String TOTAL_SECONDS = "total_seconds";
  private static final String ROW_COUNT = "row_count";

  private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)(B|KB|MB|GB|TB)");
  private static final Pattern TIME_DURATION_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)(s|m|h|d)");

  private String byteSizeUnit;
  private String timeDurationUnit;
  private String column;
  private String sizeColumn;
  private String durationColumn;
  private String sizeOutput;
  private String timeOutput;
  private String aggregationType;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    // First two arguments are column names with colon prefix
    builder.define("col1", TokenType.COLUMN_NAME);
    builder.define("col2", TokenType.COLUMN_NAME);
    // Next two arguments are output column names
    builder.define("out1", TokenType.TEXT);
    builder.define("out2", TokenType.TEXT);
    // Named parameters with defaults
    builder.define("size-unit", TokenType.TEXT, "MB");
    builder.define("time-unit", TokenType.TEXT, "seconds");
    builder.define("aggregation-type", TokenType.TEXT, "total");
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    // Get the column names - they should be prefixed with ':'
    String col1 = ((ColumnName) args.value("col1")).value();
    String col2 = ((ColumnName) args.value("col2")).value();
    if (!col1.startsWith(":") || !col2.startsWith(":")) {
      throw new DirectiveParseException("Column names must be prefixed with ':'");
    }
    this.sizeColumn = col1.substring(1);
    this.durationColumn = col2.substring(1);
    
    // Get the output column names
    this.sizeOutput = ((Text) args.value("out1")).value();
    this.timeOutput = ((Text) args.value("out2")).value();
    
    // Get the size unit and validate it
    String sizeUnit = ((Text) args.value("size-unit")).value().toUpperCase();
    if (!sizeUnit.matches("B|KB|MB|GB|TB")) {
      throw new DirectiveParseException(
        String.format("Invalid size unit '%s'. Must be one of: B, KB, MB, GB, TB", sizeUnit));
    }
    this.byteSizeUnit = sizeUnit;
    
    // Get the time unit and validate it
    this.timeDurationUnit = ((Text) args.value("time-unit")).value().toLowerCase();
    if (!timeDurationUnit.matches("s|seconds|m|minutes|h|hours|d|days")) {
      throw new DirectiveParseException(
        "Invalid time unit '" + timeDurationUnit + "'. " +
        "Must be one of: s, seconds, m, minutes, h, hours, d, days");
    }
    
    // Get the aggregation type
    this.aggregationType = ((Text) args.value("aggregation-type")).value();
    if (!aggregationType.matches("total|average")) {
      throw new DirectiveParseException(
        String.format("Invalid aggregation type '%s'. Must be one of: total, average", aggregationType));
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException {
    long totalBytes = 0;
    long totalMilliseconds = 0;
    int rowCount = 0;

    for (Row row : rows) {
      Object value = row.getValue(sizeColumn);
      if (value != null) {
        try {
          if (value instanceof Number) {
            totalBytes += ((Number) value).longValue();
          } else if (value instanceof String) {
            String strValue = (String) value;
            if (strValue.matches("\\d+(?:\\.\\d+)?(?:B|KB|MB|GB|TB)")) {
              totalBytes += new ByteSize(strValue).getBytes();
            } else {
              // Skip invalid values
              continue;
            }
          }
        } catch (Exception e) {
          // Skip invalid values
          continue;
        }
      }

      Object duration = row.getValue(durationColumn);
      if (duration != null) {
        try {
          if (duration instanceof Number) {
            totalMilliseconds += ((Number) duration).longValue() * 1000;
          } else if (duration instanceof String) {
            String strDuration = (String) duration;
            if (strDuration.matches("\\d+(?:\\.\\d+)?(?:s|m|h|d)")) {
              totalMilliseconds += new TimeDuration(strDuration).getMilliseconds();
            } else {
              // Skip invalid values
              continue;
            }
          }
        } catch (Exception e) {
          // Skip invalid values
          continue;
        }
      }
      rowCount++;
    }

    // Update context properties
    if (context != null) {
      TransientStore store = context.getTransientStore();
      store.set(TransientVariableScope.GLOBAL, TOTAL_BYTES, totalBytes);
      store.set(TransientVariableScope.GLOBAL, TOTAL_SECONDS, totalMilliseconds / 1000);
      store.set(TransientVariableScope.GLOBAL, ROW_COUNT, rowCount);
    }

    // Create a new row with the aggregated results
    Row result = new Row();
    if (rowCount == 0) {
      result.add(sizeOutput, "0" + byteSizeUnit);
      result.add(timeOutput, "0" + timeDurationUnit);
      result.add("row_count", 0);
      return Collections.singletonList(result);
    }

    if ("average".equals(aggregationType)) {
      result.add(sizeOutput, new ByteSize((totalBytes / rowCount) + "B").convertTo(byteSizeUnit));
      result.add(timeOutput, new TimeDuration((totalMilliseconds / rowCount) + "ms").convertTo(timeDurationUnit));
    } else {
      result.add(sizeOutput, new ByteSize(totalBytes + "B").convertTo(byteSizeUnit));
      result.add(timeOutput, new TimeDuration(totalMilliseconds + "ms").convertTo(timeDurationUnit));
    }
    result.add("row_count", rowCount);

    return Collections.singletonList(result);
  }

  @Override
  public void destroy() {
    // no-op
  }
} 

