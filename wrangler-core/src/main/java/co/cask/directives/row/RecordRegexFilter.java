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

package co.cask.directives.row;

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
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A Wrangle step for filtering rows that match the pattern specified on the column.
 */
@Plugin(type = Directive.TYPE)
@Name(RecordRegexFilter.NAME)
@Categories(categories = { "row", "data-quality"})
@Description("Filters rows if the regex is matched or not matched.")
public class RecordRegexFilter implements Directive {
  public static final String NAME = "filter-by-regex";
  private String column;
  private Pattern pattern;
  private boolean matched = false;

  // filter-by-regex if-matched :column 'expression'
  // filter-by-regex if-not-matched :column 'expression'
  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("match-type", TokenType.IDENTIFIER);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("regex", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    String matchType = ((Identifier) args.value("match-type")).value();
    if (matchType.equalsIgnoreCase("if-matched")) {
      matched = true;
    } else if (matchType.equalsIgnoreCase("if-not-matched")) {
      matched = false;
    } else {
      throw new DirectiveParseException("Match type specified is not 'if-matched' or 'if-not-matched'");
    }
    column = ((ColumnName) args.value("column")).value();
    String regex = ((Text) args.value("regex")).value();
    if (!regex.equalsIgnoreCase("null") && !regex.isEmpty()) {
      pattern = Pattern.compile(regex);
    } else {
      pattern = null;
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    if (pattern == null) {
      return rows;
    }
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof JSONObject) {
          if (pattern == null && JSONObject.NULL.equals(object)) {
            continue;
          }
        } else if (object instanceof String) {
          if (matchPattern((String) row.getValue(idx))) {
            continue;
          }
        } else if (object instanceof Number) {
          if (matchPattern((row.getValue(idx)).toString())) {
            continue;
          }
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid value type '%s' of column '%s'. Should be of type String.",
                          toString(), object != null ? object.getClass().getName() : "null", column)
          );
        }
        results.add(row);
      } else {
        results.add(row);
      }
    }
    return results;
  }

  private boolean matchPattern(String value) {
    boolean matches = pattern.matcher(value).matches(); // pattern.matcher(value).matches();
    if (!matched) {
      matches = !matches;
    }
    return matches;
  }
}

