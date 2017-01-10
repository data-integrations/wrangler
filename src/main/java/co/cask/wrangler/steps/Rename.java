/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.StepException;

/**
 * Wrangle Step for renaming a column.
 */
public class Rename extends AbstractStep {
  // Columns of the columns that needs to be renamed.
  private String source;

  // Columns of the column to be renamed to.
  private String destination;

  public Rename(int lineno, String detail, String source, String destination) {
    super(lineno, detail);
    this.source = source;
    this.destination = destination;
  }

  /**
   * Renames the column from 'source' to 'destination'.
   * If the source column doesn't exist, then it will return the row as it.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return Transformed {@link Row} with column name modified.
   * @throws StepException Thrown when there is no 'source' column in the row.
   */
  @Override
  public Row execute(Row row) throws StepException, SkipRowException {
    int idx = row.find(source);
    if (idx != -1) {
      row.setColumn(idx, destination);
    } else {
      throw new StepException(toString() + " : " +
        source + " column is not defined. Please check the wrangling steps."
      );
    }
    return row;
  }
}
