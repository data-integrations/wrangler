/*
 *  Copyright © 2021 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */
package io.cdap.directives.datetime;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.FormatStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;

/**
 * Directive to format a datetime column as a string in the specified format
 */
@Plugin(type = Directive.TYPE)
@Name("format-datetime")
@Categories(categories = {"format", "datetime"})
@Description("Formats a datetime value to a string using the given format")
public class FormatDateTime implements Directive, Lineage {

  public static final String NAME = "format-datetime";
  private static final String COLUMN = "column";
  private static final String FORMAT = "format";
  private String column;
  private String format;
  private DateTimeFormatter formatter;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define(COLUMN, TokenType.COLUMN_NAME);
    builder.define(FORMAT, TokenType.TEXT, Optional.FALSE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value(COLUMN)).value();
    this.format = args.value(FORMAT).value().toString();
    try {
      // If format contains 'a', create a formatter with uppercase AM/PM markers
      if (this.format.contains("a")) {
        // Replace the 'a' pattern letter with a custom uppercase AM/PM formatter
        String patternWithoutA = this.format.replace("a", "");
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        
        int aPosition = this.format.indexOf("a");
        if (aPosition > 0) {
          builder.appendPattern(this.format.substring(0, aPosition));
        }
        
        // Add uppercase AM/PM marker
        builder.appendText(ChronoField.AMPM_OF_DAY, TextStyle.SHORT);
        
        if (aPosition < this.format.length() - 1) {
          builder.appendPattern(this.format.substring(aPosition + 1));
        }
        
        this.formatter = builder.toFormatter(Locale.ENGLISH);
      } else {
        this.formatter = DateTimeFormatter.ofPattern(this.format);
      }
    } catch (IllegalArgumentException exception) {
      throw new DirectiveParseException(NAME, String.format("Datetime format '%s' is invalid.", this.format),
                                        exception);
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws ErrorRowException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx == -1) {
        continue;
      }
      Object value = row.getValue(idx);
      // If the data in the cell is null, then skip this row.
      if (value == null) {
        continue;
      }

      if (!(value instanceof LocalDateTime)) {
        throw new ErrorRowException(NAME, String.format("Value %s for column %s expected to be datetime but found %s",
                                                        value.toString(), column, value.getClass().getSimpleName()), 2);
      }

      try {
        LocalDateTime localDateTime = (LocalDateTime) value;
        row.setValue(idx, localDateTime.format(formatter));
      } catch (DateTimeException exception) {
        throw new ErrorRowException(NAME, String.format("Error converting datetime %s to string with format %s",
                                                        value.toString(), format), 2, exception);
      }
    }
    return rows;
  }

  @Override
  public void destroy() {
    //no op
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Datetime column '%s' converted to string with format '%s'", column, format)
      .relation(column, column)
      .build();
  }
}
