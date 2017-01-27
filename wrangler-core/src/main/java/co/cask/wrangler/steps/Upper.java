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
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.SkipRecordException;
import co.cask.wrangler.api.StepException;

/**
 * A Wrangler step for upper casing the 'col' value of type String.
 */
public class Upper extends AbstractStep {
  // Columns of the column to be upper cased.
  private String col;

  public Upper(int lineno, String detail, String col) {
    super(lineno, detail);
    this.col = col;
  }

  /**
   * Transforms a column value from any case to upper case.
   *
   * @param record Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Record} in which the 'col' value is lower cased.
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public Record execute(Record record, PipelineContext context) throws StepException, SkipRecordException {
    int idx = record.find(col);

    if (idx != -1) {
      String value = (String) record.getValue(idx);
      if (value != null) {
        record.setValue(idx, value.toUpperCase());
      }
    } else {
      throw new StepException(toString() + " : " +
        col + " was not found or is not of type string. Please check the wrangle configuration."
      );
    }
    return record;
  }
}
