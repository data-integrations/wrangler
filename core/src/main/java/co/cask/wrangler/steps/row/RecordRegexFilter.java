/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.row;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A Wrangle step for filtering rows that match the pattern specified on the column.
 */
@Plugin(type = "directives")
@Name("filter-row-if-matched")
@Usage("filter-row-if-matched <column> <regex>")
@Description("[DEPRECATED] Filters rows if the regex is matched. Use 'filter-rows-on' instead.")
public class RecordRegexFilter extends AbstractDirective {
  private final String regex;
  private final String column;
  private Pattern pattern;
  private boolean match;

  public RecordRegexFilter(int lineno, String detail, String column, String regex, boolean match) {
    super(lineno, detail);
    this.regex = regex.trim();
    this.column = column;
    this.match = match;
    if (!regex.equalsIgnoreCase("null")) {
      pattern = Pattern.compile(this.regex);
    } else {
      pattern = null;
    }
  }

  /**
   * Sets the new column names for the {@link Row}.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Row}.
   * @throws DirectiveExecutionException
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    if (pattern == null) {
      return rows;
    }
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object == null) {
          if(!match)
            continue;
        } else if (object instanceof JSONObject) {
          if (pattern == null && JSONObject.NULL.equals(object)) {
            continue;
          }
        } else if (object instanceof String) {
          String value = (String) row.getValue(idx);
          boolean status = pattern.matcher(value).matches(); // pattern.matcher(value).matches();
          if (!match) {
            status = !status;
          }
          if (status) {
            continue;
          }
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid value type '%s' of column '%s'. Should be of type String.",
                          toString(), object != null ? object.getClass().getName() : "null", column)
          );
        }
        results.add(row);
      }
    }
    return results;
  }
}

