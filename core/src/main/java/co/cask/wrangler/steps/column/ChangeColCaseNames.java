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

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.List;

/**
 * Applies a sed expression on the column names.
 *
 * This directive helps clearing out the columns names to make it more readable.
 */
@Usage(
  directive = "change-column-case",
  usage = "change-column-case <upper|lower>",
  description = "Changes the case of the columns to lowercase or uppercase."
)
public class ChangeColCaseNames extends AbstractStep {
  private final boolean lowerCase;

  public ChangeColCaseNames(int lineno, String detail, String colCase) {
    super(lineno, detail);
    boolean lower = false;
    if (colCase != null || colCase.isEmpty()) {
      lower = true;
    } else if (colCase.equalsIgnoreCase("lower") || colCase.equalsIgnoreCase("lowercase")) {
      lower = true;
    }
    lowerCase = lower;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      for (int i = 0; i < record.length(); ++i) {
        String name = record.getColumn(i);
        if (lowerCase) {
          record.setColumn(i, name.toLowerCase());
        } else {
          record.setColumn(i, name.toUpperCase());
        }
      }
    }
    return records;
  }
}

