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
package io.cdap.directives.parser;

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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Directive for parsing a string in the specified format to DateTime.
 */
@Plugin(type = Directive.TYPE)
@Name("parse-as-datetime")
@Categories(categories = {"parser", "datetime"})
@Description("Parse a column value as datetime using the given format")
public class ParseDateTime implements Directive, Lineage {

  public static final String NAME = "parse-as-datetime";
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
      this.formatter = DateTimeFormatter.ofPattern(this.format);
    } catch (IllegalArgumentException exception) {
      throw new DirectiveParseException(NAME, String.format("'%s' is an invalid datetime format.", this.format),
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
      // If the data in the cell is null or is already Datetime , then skip this row.
      if (value == null || value instanceof LocalDateTime) {
        continue;
      }

      try {
        LocalDateTime localDateTime = LocalDateTime.parse(value.toString(), formatter);
        row.setValue(idx, localDateTime);
      } catch (DateTimeParseException exception) {
        throw new ErrorRowException(NAME, String.format("Value %s for column %s is not in expected format %s",
                                                        value.toString(), column, format), 2, exception);
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
      .readable("Parsed column '%s' in format '%s' as datetime", column, format)
      .relation(column, column)
      .build();
  }
}
