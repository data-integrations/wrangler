/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.date;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Identifier;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.List;

/**
 * A directive for taking difference in Dates.
 */
@Plugin(type = Directive.Type)
@Name("get-date")
@Categories(categories = {"date"})
@Description("")
public class GetDate implements Directive {
  public static final String NAME = "get-date";
  private String source;
  private String destination;
  private String op;
  private final Date date = new Date();

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    builder.define("op", TokenType.IDENTIFIER);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.source = ((ColumnName) args.value("source")).value();
    this.destination = ((ColumnName) args.value("destination")).value();
    if (args.contains("op")) {
      String o = ((Identifier) args.value("op")).value();
      o = o.toLowerCase();
      switch (o) {
      }
    } else {
      this.op = "milliseconds";
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(source);

      if (idx == -1) {
        continue;
      }

      Object o = row.getColumn(idx);
      if (o == null) {
        continue;
      }

      if (!(o instanceof DateTime)) {
        continue;
      }

      DateTime value = (DateTime) o;

      switch(op) {
        case "day-of-month":
          row.addOrSet(destination, value.getDayOfMonth());
          break;

        case "day-of-week":
          row.addOrSet(destination, value.getDayOfWeek());
          break;

        case "day-of-year":
          row.addOrSet(destination, value.getDayOfYear());
          break;

        case "hour-of-day":
          row.addOrSet(destination, value.getHourOfDay());
          break;

        case "minute-of-day":
          row.addOrSet(destination, value.getMinuteOfDay());
          break;

        case "second-of-minute":
          row.addOrSet(destination, value.getSecondOfMinute());
          break;

        case "century-of-era":
          row.addOrSet(destination, value.getCenturyOfEra());
          break;

        case "month-of-year":
          row.addOrSet(destination, value.getMonthOfYear());
          break;
      }
    }
    return rows;
  }

  private DateTime getDate(Row row, String colName) throws DirectiveExecutionException {
    // If one of the column contains now, then we return
    // the current date.
    if (colName.equalsIgnoreCase("now")) {
      return new DateTime(date);
    }

    // Else attempt to find the column.
    int idx = row.find(colName);
    if (idx == -1) {
      throw new DirectiveExecutionException(toString() + " : '" +
                                colName + "' column is not defined in the row.");
    }
    Object o = row.getValue(idx);
    if (o == null || !(o instanceof Date)) {
      return null;
    }
    return new DateTime(o);
  }
}
