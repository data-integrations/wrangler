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
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * A directive for taking difference in Dates.
 */
@Plugin(type = Directive.TYPE)
@Name("diff-date")
@Categories(categories = {"date"})
@Description("Calculates the difference in milliseconds between two Date objects." +
  "Positive if <column2> earlier. Must use 'parse-as-date' or 'parse-as-simple-date' first.")
public class DiffDate implements Directive, Lineage {
  public static final String NAME = "diff-date";
  private String column1;
  private String column2;
  private String destCol;
  // Timestamp now UTC
  private final ZonedDateTime date = ZonedDateTime.now(ZoneId.ofOffset("UTC", ZoneOffset.UTC));

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column1", TokenType.COLUMN_NAME);
    builder.define("column2", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column1 = ((ColumnName) args.value("column1")).value();
    this.column2 = ((ColumnName) args.value("column2")).value();
    this.destCol = ((ColumnName) args.value("destination")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      ZonedDateTime date1 = getDate(row, column1);
      ZonedDateTime date2 = getDate(row, column2);
      if (date1 != null && date2 != null) {
        row.addOrSet(destCol, date1.toInstant().toEpochMilli() - date2.toInstant().toEpochMilli());
      } else {
        row.addOrSet(destCol, null);
      }
    }
    return rows;
  }

  private ZonedDateTime getDate(Row row, String colName) throws DirectiveExecutionException {
    // If one of the column contains now, then we return
    // the current date.
    if (colName.equalsIgnoreCase("now")) {
      return date;
    }

    // Else attempt to find the column.
    int idx = row.find(colName);
    if (idx == -1) {
      throw new DirectiveExecutionException(NAME, "Column '" + colName + "' does not exist.");
    }
    Object o = row.getValue(idx);
    if (o == null || !(o instanceof ZonedDateTime)) {
      return null;
    }
    return (ZonedDateTime) o;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Calculated difference between dates in column '%s' and '%s' and store in '%s'"
        , column1, column2, destCol)
      .relation(Many.columns(column1, column2), destCol)
      .relation(column1, column1)
      .relation(column2, column2)
      .build();
  }
}
