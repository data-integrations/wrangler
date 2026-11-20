/*
 *  Copyright © 2018-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.parser;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A Executor to parse timestamp as date.
 */
@Plugin(type = Directive.TYPE)
@Name("parse-timestamp")
@Categories(categories = {"parser", "date"})
@Description("Parses column values representing unix timestamp as date.")
public class ParseTimestamp implements Directive, Lineage {
  public static final String NAME = "parse-timestamp";
  private static final Set<TimeUnit> SUPPORTED_TIME_UNITS = EnumSet.of(TimeUnit.SECONDS, TimeUnit.MILLISECONDS,
                                                                       TimeUnit.MICROSECONDS);
  private String column;
  private TimeUnit timeUnit;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("timeunit", TokenType.TIME_DURATION, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.timeUnit = TimeUnit.MILLISECONDS;

    if (args.contains("timeunit")) {
      String value = ((TimeDuration) args.value("timeunit")).value();
      String unitValue = value.replaceAll("[0-9.]", ""); // Extract just the unit part
      try {
        TimeUnit unit = TimeUnit.valueOf(unitValue.toUpperCase());
        if (!SUPPORTED_TIME_UNITS.contains(unit)) {
          throw new DirectiveParseException(
            NAME, String.format("Time unit '%s' is not a supported time unit. Supported time units are %s",
                            unitValue, SUPPORTED_TIME_UNITS));
        }
        this.timeUnit = unit;
      } catch (IllegalArgumentException e) {
        throw new DirectiveParseException(
          NAME, String.format("Time unit '%s' is not a supported time unit. Supported time units are %s",
                          unitValue, SUPPORTED_TIME_UNITS), e);
      }
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        // If the data in the cell is null or is already of date format, then
        // continue to next row.
        if (object == null || object instanceof ZonedDateTime) {
          continue;
        }

        long longValue = getLongValue(object);
        ZonedDateTime zonedDateTime = getZonedDateTime(longValue, timeUnit, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
        row.setValue(idx, zonedDateTime);
      }
    }
    return rows;
  }

  private long getLongValue(Object object) throws ErrorRowException {
    String errorMsg = String.format("Invalid type '%s' of column '%s'. Must be of type 'Long' or 'String'.",
                                  object.getClass().getSimpleName(), column);
    try {
      if (object instanceof Long) {
        return (long) object;
      } else if (object instanceof String) {
        return Long.parseLong((String) object);
      }
    } catch (Exception e) {
      errorMsg = String.format("Invalid value for column '%s'. Must be of type 'Long' or 'String' " +
                             "representing long.", column);
    }
    throw new ErrorRowException(NAME, errorMsg, 2);
  }

  private ZonedDateTime getZonedDateTime(long ts, TimeUnit unit, ZoneId zoneId) {
    switch (unit) {
      case SECONDS:
        return Instant.ofEpochSecond(ts).atZone(zoneId);
      case MILLISECONDS:
        return Instant.ofEpochMilli(ts).atZone(zoneId);
      case MICROSECONDS:
        long epochSeconds = ts / 1_000_000;
        long nanos = (ts % 1_000_000) * 1_000;
        return Instant.ofEpochSecond(epochSeconds, nanos).atZone(zoneId);
      default:
        // This shouldn't happen since we validate in initialize()
        throw new IllegalStateException("Unsupported time unit: " + unit);
    }
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Parsed column '%s' as a timestamp using time unit as '%s'", column, timeUnit)
      .relation(column, column)
      .build();
  }
}
