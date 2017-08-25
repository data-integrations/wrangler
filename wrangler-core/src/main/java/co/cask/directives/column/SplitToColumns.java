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

package co.cask.directives.column;

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
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for splitting the columns into multiple columns.
 */
@Plugin(type = Directive.Type)
@Name(SplitToColumns.NAME)
@Categories(categories = { "column"})
@Description("Splits a column into one or more columns around matches of the specified regular expression.")
public class SplitToColumns implements Directive {
  public static final String NAME = "split-to-columns";
  // Column on which to apply mask.
  private String column;

  // Type of mask.
  private String regex;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("regex", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    column = ((ColumnName) args.value("column")).value();
    regex = ((Text) args.value("regex")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();

    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof String) {
          String[] lines = ((String) object).split(regex);
          int i = 1;
          for (String line : lines) {
            row.add(String.format("%s_%d", column, i), line);
            ++i;
          }
          results.add(row);
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          object != null ? object.getClass().getName() : "null", column)
          );
        }
      }
    }
    return results;
  }
}

