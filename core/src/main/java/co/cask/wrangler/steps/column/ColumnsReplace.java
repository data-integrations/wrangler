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

package co.cask.wrangler.steps.column;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
import org.unix4j.Unix4j;
import org.unix4j.builder.Unix4jCommandBuilder;

import java.util.List;

/**
 * Applies a sed expression on the column names.
 *
 * This directive helps clearing out the columns names to make it more readable.
 */

@Plugin(type = "udd")
@Name("columns-replace")
@Usage("columns-replace <sed-expression>")
@Description("Modifies column names in bulk using a sed-format expression.")
public class ColumnsReplace extends AbstractDirective {
  private final String sed;

  public ColumnsReplace(int lineno, String detail, String sed) {
    super(lineno, detail);
    this.sed = sed.trim();
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows List of input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled List of {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      for (int i = 0; i < row.length(); ++i) {
        String name = row.getColumn(i);
        try {
          Unix4jCommandBuilder builder = Unix4j.echo(name).sed(sed);
          row.setColumn(i, builder.toStringResult());
        } catch (IllegalArgumentException e) {
          throw new DirectiveExecutionException(
            String.format(toString() + " : " + e.getMessage())
          );
        }
      }
    }
    return rows;
  }
}

