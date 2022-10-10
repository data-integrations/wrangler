/*
 * Copyright © 2017-2022 Cask Data, Inc.
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
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

/**
 * A Executor to parse date into {@link ZonedDateTime} object.
 */
@Plugin(type = Directive.TYPE)
@Name("parse-as-simple-date")
@Categories(categories = {"parser", "date"})
@Description("Parses a column as date using format.")
public class ParseSimpleDate implements Directive, Lineage {
  public static final String NAME = "parse-as-simple-date";
  private String column;
  private SimpleDateFormat formatter;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("format", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    String format = ((Text) args.value("format")).value();
    this.formatter = new SimpleDateFormat(format);
    // CDAP-19615 Use pure Gregorian Calendar to avoid Julian date precision loss
    GregorianCalendar gc = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    gc.setGregorianChange(new Date(Long.MIN_VALUE));
    formatter.setCalendar(gc);
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
        if (object instanceof String) {
          try {
            // This implementation first creates Date object and then converts it into ZonedDateTime. This is because
            // ZonedDateTime requires presence of Zone and Time components in the pattern and object to be parsed.
            // For example if the pattern is yyyy-mm-dd, ZonedDateTime object can not be created and the call to
            // ZonedDateTime.parse("2018-12-21", formatter) will throw DateTimeParseException
            Date date = formatter.parse(object.toString());
            ZonedDateTime zonedDateTime = ZonedDateTime.from(date.toInstant()
                                                               .atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
            row.setValue(idx, zonedDateTime);
          } catch (ParseException e) {
            throw new ErrorRowException(
              NAME, String.format("Failed to parse '%s' with pattern '%s'", object, formatter.toPattern()), 1);
          }
        } else {
          throw new ErrorRowException(
            NAME, String.format("Column '%s' is of invalid type '%s'. It should be of type 'String'.",
                                column, object.getClass().getSimpleName()), 2);
        }
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Parsed column '%s' as date using user specified format '%s'", column, formatter.toPattern())
      .relation(column, column)
      .build();
  }
}
