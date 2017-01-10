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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A Wrangle step for setting the columns obtained form wrangling.
 *
 * This step will create a copy of the input {@link Row} and clears
 * all previous column names and add new column names.
 */
public class Columns extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(Columns.class);

  // Name of the columns represented in a {@link Row}
  private List<String> columns;

  // Replaces the input {@link Row} column names.
  boolean replaceColumnNames;

  public Columns(int lineno, String detail, List<String> columns) {
    this(lineno, detail, columns, true);
  }

  public Columns(int lineno, String detail, List<String> columns, boolean replaceColumnNames) {
    super(lineno, detail);
    this.replaceColumnNames = replaceColumnNames;
    this.columns = columns;
  }

  /**
   * Sets the new column names for the {@link Row}.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return A newly transformed {@link Row}.
   * @throws StepException
   */
  @Override
  public Row execute(Row row) throws StepException, SkipRowException {
    Row r = new Row(row);
    if (replaceColumnNames) {
      r.clearColumns();
    }
    for (String name : columns) {
      r.addColumn(name);
    }
    return r;
  }
}

