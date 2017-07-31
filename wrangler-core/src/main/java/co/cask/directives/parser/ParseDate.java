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

package co.cask.directives.parser;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.lineage.MutationDefinition;
import co.cask.wrangler.api.lineage.MutationType;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * A Executor to parse date.
 */
@Plugin(type = Directive.Type)
@Name("parse-as-date")
@Categories(categories = { "parser", "date"})
@Description("Parses column values as dates using natural language processing and " +
  "automatically identifying the format (expensive in terms of time consumed).")
public class ParseDate implements Directive {
  public static final String NAME = "parse-as-date";
  private String column;
  private String timezone;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("timezone", TokenType.TEXT, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    if (args.contains("timezone")) {
      this.timezone = ((Text) args.value("timezone")).value();
    } else {
      this.timezone = "UTC";
    }
    TimeZone.setDefault(TimeZone.getTimeZone(timezone));
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof String) {
          Parser parser = new Parser();
          List<DateGroup> groups = parser.parse((String) object);
          int i = 1;
          for (DateGroup group : groups) {
            List<Date> dates = group.getDates();
            for (Date date : dates) {
              row.add(String.format("%s_%d", column, i), date);
            }
            i++;
          }
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          object != null ? object.getClass().getName() : "null", column)
          );
        }
      } else {
        throw new DirectiveExecutionException(toString() + " : Column '" + column + "' does not exist in the row.");
      }
    }
    return rows;
  }

  @Override
  public MutationDefinition lineage() {
    MutationDefinition.Builder builder = new MutationDefinition.Builder(NAME, "Timezone: " + timezone);
    builder.addMutation(column, MutationType.READ);
    builder.addMutation("all columns formatted " + column + "_%d", MutationType.ADD);
    return builder.build();
  }
}
