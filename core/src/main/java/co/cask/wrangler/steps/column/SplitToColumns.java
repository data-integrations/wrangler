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
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.annotations.Usage;

import java.util.ArrayList;
import java.util.List;

/**
 * A Split on Stage for splitting the columns into multiple columns.
 */
@Plugin(type = "directives")
@Name("split-to-columns")
@Usage("split-to-columns <column> <regex>")
@Description("Splits a column into one or more columns around matches of the specified regular expression.")
public class SplitToColumns extends AbstractDirective {
  // Column on which to apply mask.
  private final String column;

  // Type of mask.
  private final String regex;

  public SplitToColumns(int lineno, String detail, String column, String regex) {
    super(lineno, detail);
    this.column = column;
    this.regex = regex;
  }

  /**
   * Splits a record into multiple columns based on delimiter.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Row} with masked column.
   * @throws DirectiveExecutionException thrown when there is issue with masking
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
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

