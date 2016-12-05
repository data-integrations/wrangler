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

import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * A Wrangle step for dropping the columns obtained form wrangling.
 *
 * This step will create a copy of the input {@link Row} and clears
 * all previous column names and add new column names.
 */
public class Drop implements Step {
  private static final Logger LOG = LoggerFactory.getLogger(Drop.class);

  // Columns to be dropped.
  private List<String> columns;

  public Drop(String column) {
    this(Arrays.asList(column));
  }

  public Drop(List<String> columns) {
    this.columns = columns;
  }

  /**
   * Drops the columns specified.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return A newly transformed {@link Row}.
   * @throws StepException
   */
  @Override
  public Row execute(Row row) throws StepException {
    for(String column : columns) {
      int idx = row.find(column);
      row.remove(idx);
    }
    return row;
  }
}

