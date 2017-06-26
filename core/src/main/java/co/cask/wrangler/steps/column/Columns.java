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

package co.cask.wrangler.steps.column;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Usage;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangle step for setting the columns obtained from wrangling.
 *
 * This step will create a copy of the input {@link Row} and clears
 * all previous column names and add new column names.
 */

@Plugin(type = "udd")
@Name("set columns")
@Usage("set columns <columm>[,<column>*]")
@Description("Sets the name of columns, in the order they are specified.")
public class Columns extends AbstractDirective {
  // Name of the columns represented in a {@link Row}
  private List<String> columns = new ArrayList<>();

  // Replaces the input {@link Row} column names.
  private boolean replaceColumnNames;

  public Columns(int lineno, String detail, List<String> columns) throws DirectiveParseException {
    this(lineno, detail, columns, true);
  }

  public Columns(int lineno, String detail, List<String> columns, boolean replaceColumnNames)
    throws DirectiveParseException {
    super(lineno, detail);
    this.replaceColumnNames = replaceColumnNames;
    int loc = 1;
    for (String column : columns) {
      column = column.replaceAll("\"|'", "");
      column = column.trim();
      if (column.isEmpty()) {
        throw new DirectiveParseException(
          String.format("Column at location %d is empty. Cannot have empty column names.", loc)
        );
      }
      loc++;
      this.columns.add(column);
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
  public List<Row> execute(List<Row> rows, RecipeContext context)
    throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = 0;
      for (String name : columns) {
        if (idx < row.length()) {
          row.setColumn(idx, name.trim());
        }
        idx++;
      }
    }
    return rows;
  }
}

