/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.parser;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.annotations.Usage;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * A Directive to parse date.
 */
@Plugin(type = "udd")
@Name("parse-as-date")
@Usage("parse-as-date <column> [<timezone>]")
@Description("Parses column values as dates using natural language processing and " +
  "automatically identifying the format (expensive in terms of time consumed).")
public class ParseDate extends AbstractDirective {
  private final String column;
  private final String timezone;

  public ParseDate(int lineno, String directive, String column, String timezone) {
    super(lineno, directive);
    this.column = column;
    this.timezone = timezone;
    if(timezone == null) {
      timezone = "UTC";
    }
    // this is bad. esp if you have more than one such step in your pipeline
    TimeZone.setDefault(TimeZone.getTimeZone(timezone));
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows  Input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
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
}
