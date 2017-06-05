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
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.api.i18n.Messages;
import co.cask.wrangler.api.i18n.MessagesFactory;

import java.util.List;

/**
 * Wrangle Step for renaming a column.
 */
@Plugin(type = "udd")
@Name("rename")
@Usage("rename <old> <new>")
@Description("Renames an existing column.")
public class Rename extends AbstractStep {
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
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Record} with column name modified.
   * @throws StepException Thrown when there is no 'source' column in the record.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(oldcol);
      int idxnew = record.find(newcol);
      if (idx != -1) {
        if (idxnew == -1) {
          record.setColumn(idx, newcol);
        } else {
          throw new StepException(
            String.format(
              "%s : %s column already exists. Apply the directive 'drop %s' before renaming %s to %s.", toString(),
              newcol, newcol, oldcol, newcol
            )
          );
        }
      }
    }
    return records;
  }
}
