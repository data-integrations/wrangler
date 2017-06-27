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
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
import co.cask.wrangler.i18n.Messages;
import co.cask.wrangler.i18n.MessagesFactory;

import java.util.List;

/**
 * Wrangle Directive for renaming a column.
 */
@Plugin(type = "directives")
@Name("rename")
@Usage("rename <old> <new>")
@Description("Renames an existing column.")
public class Rename extends AbstractDirective {
  private static final Messages MSG = MessagesFactory.getMessages();

  // Columns of the columns that needs to be renamed.
  private String oldcol;

  // Columns of the column to be renamed to.
  private String newcol;

  public Rename(int lineno, String detail, String oldcol, String newcol) {
    super(lineno, detail);
    this.oldcol = oldcol;
    this.newcol = newcol;
  }

  /**
   * Renames the column from 'old' to 'new'.
   * If the source column doesn't exist, then it will return the record as it.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Row} with column name modified.
   * @throws DirectiveExecutionException Thrown when there is no 'source' column in the record.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(oldcol);
      int idxnew = row.find(newcol);
      if (idx != -1) {
        if (idxnew == -1) {
          row.setColumn(idx, newcol);
        } else {
          throw new DirectiveExecutionException(
            String.format(
              "%s : %s column already exists. Apply the directive 'drop %s' before renaming %s to %s.", toString(),
              newcol, newcol, oldcol, newcol
            )
          );
        }
      }
    }
    return rows;
  }
}
