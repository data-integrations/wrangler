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
import co.cask.wrangler.api.StepException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Wrangler step for upper casing the 'col' value of type String.
 */
public class Upper extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(Columns.class);

  // Columns of the column to be upper cased.
  private String col;

  public Upper(int lineno, String detail, String col) {
    super(lineno, detail);
    this.col = col;
  }

  /**
   * Transforms a column value from any case to upper case.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return Transformed {@link Row} in which the 'col' value is lower cased.
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public Row execute(Row row) throws StepException {
    int idx = row.find(col);

    if (idx != -1) {
      String value = (String) row.getValue(idx);
      if (value != null) {
        row.setValue(idx, value.toUpperCase());
      }
    } else {
      throw new StepException(toString() + " : " +
        col + " was not found or is not of type string. Please check the wrangle configuration."
      );
    }
    return row;
  }
}
