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

package io.cdap.directives.row;

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
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
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
public class RecordRegexFilter implements Directive, Lineage {
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
      throw new DirectiveParseException(
        NAME, "Match type specified is not 'if-matched' or 'if-not-matched'");
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

        if (object == null) {
          throw new DirectiveExecutionException(
            NAME, String.format("Column '%s' has null value. It should be a non-null 'String', " +
                                  "'JSONObject' or 'Number'.", column));
        }

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
            NAME, String.format("Column '%s' is of invalid type '%s'. It should be of type " +
                                  "'String', 'JSONObject' or 'Number'.", column, object.getClass().getSimpleName())
          );
        }
        results.add(row);
      } else {
        results.add(row);
      }
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Filtered column '%s' based on whether the expression '%s' %s ",
                column, pattern, matched ? "matched" : "not matched")
      .relation(column, column)
      .build();
  }

  private boolean matchPattern(String value) {
    boolean matches = pattern.matcher(value).matches(); // pattern.matcher(value).matches();
    if (!matched) {
      matches = !matches;
    }
    return matches;
  }
}

