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
import com.google.common.base.CaseFormat;

import java.util.List;

/**
 * Converts the case of a column based on {@CaseFormat} formats
 *
 * This directive helps standardizing columns names to make them more readable.
 */
@Usage(directive = "columns-format-case", usage = "columns-format-case <current-case> <desired-case>")
public class ColumnsFormatCase extends AbstractStep {
  private final CaseFormat currentCase;
  private final CaseFormat desiredCase;

  public ColumnsFormatCase(int lineno, String detail, String currentCase, String desiredCase) {
    super(lineno, detail);
    this.currentCase = CaseFormat.valueOf(currentCase.toUpperCase());
    this.desiredCase = CaseFormat.valueOf(desiredCase.toUpperCase());
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
        try {
          record.setColumn(i, currentCase.to(desiredCase, name));
        } catch (IllegalArgumentException e) {
          throw new StepException(
            String.format(toString() + " : " + e.getMessage())
          );
        }
      }
    }
    return records;
  }
}

