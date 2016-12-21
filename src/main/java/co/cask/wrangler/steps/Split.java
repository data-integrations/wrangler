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
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Wrangler step for splitting a col into two additional columns based on a delimiter.
 */
public class Split extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(Split.class);

  // Name of the column to be split
  private String col;

  private String delimiter;

  // Destination column names
  private String firstColumnName, secondColumnName;

  public Split(int lineno, String detail, String col,
               String delimiter, String firstColumnName, String secondColumnName) {
    super(lineno, detail);
    this.col = col;
    this.delimiter = delimiter;
    this.firstColumnName = firstColumnName;
    this.secondColumnName = secondColumnName;
  }

  /**
   * Splits column based on the delimiter into two columns.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return Transformed {@link Row} which contains two additional columns based on the split
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public Row execute(Row row) throws StepException, SkipRowException {
    int idx = row.find(col);

    if (idx != -1) {
      String val = (String) row.getValue(idx);
      if (val != null) {
        String[] parts = val.split(delimiter, 2);
        if (Strings.isNullOrEmpty(parts[0])) {
          row.add(firstColumnName, parts[1]);
          row.add(secondColumnName, null);
        } else {
          row.add(firstColumnName, parts[0]);
          row.add(secondColumnName, parts[1]);
        }
      } else {
        row.add(firstColumnName, null);
        row.add(secondColumnName, null);
      }
    } else {
      throw new StepException(
        col + " is not of type string. Please check the wrangle configuration."
      );
    }
    return row;
  }
}
