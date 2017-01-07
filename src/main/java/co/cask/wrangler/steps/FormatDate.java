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

import java.util.regex.Pattern;

/**
 * A Wrangle step for filtering rows that match the pattern specified on the column.
 */
public class FormatDate extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(FormatDate.class);
  private final String pattern;
  private final String column;
  private Pattern matcher;

  public FormatDate(int lineno, String detail, String column, String pattern) {
    super(lineno, detail);
    this.pattern = pattern;
    this.column = column;
    matcher = Pattern.compile(pattern);
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
    int idx = row.find(column);
    if (idx != -1) {
      if (matcher.matcher((String)row.getValue(idx)).find()) {
        throw new SkipRowException();
      }
    } else {
      throw new StepException(toString() + " : '" +
                                column + "' column is not defined. Please check the wrangling step."
      );
    }
    return row;
  }
}

