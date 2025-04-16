/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.directives.date;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * A directive for managing date formats.
 */
@Plugin(type = Directive.TYPE)
@Name("format-date")
@Categories(categories = {"date", "format"})
@Description("Formats a column using a date-time format. Use 'parse-as-date` beforehand.")
public class FormatDate implements Directive, Lineage {
  public static final String NAME = "format-date";
  private String format;
  private String column;
  private DateTimeFormatter destinationFmt;

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
    this.format = ((Text) args.value("format")).value();
    this.destinationFmt = DateTimeFormatter.ofPattern(this.format);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      Row dt = new Row(row);
      int idx = dt.find(column);

      if (idx == -1) {
        throw new DirectiveExecutionException(NAME, String.format("Column '%s' does not exist.", column));
      }

      Object object = row.getValue(idx);

      if (object != null) {
        ZonedDateTime zonedDateTime;
        if (object instanceof LocalDate) {
          zonedDateTime = ((LocalDate) object).atStartOfDay(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
        } else if (object instanceof ZonedDateTime) {
          zonedDateTime = (ZonedDateTime) object;
        } else {
          throw new DirectiveExecutionException(
            NAME, String.format("Column '%s' has invalid type '%s'. Apply 'parse-as-date' directive first.",
                                column, object.getClass().getSimpleName()));
        }

        dt.setValue(idx, destinationFmt.format(zonedDateTime));
      }

      results.add(dt);
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Formatted date in column '%s' using format '%s'", column, format)
      .relation(column, column)
      .build();
  }
}
