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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.List;

/**
 * A step to fill null or empty column values with a fixed value.
 */
@Usage(directive = "fill-null-or-empty", usage = "fill-null-or-empty <column> <fixed-value>")
public class FillNullOrEmpty extends AbstractStep {
  private String column;
  private String value;

  public FillNullOrEmpty(int lineno, String detail, String column, String value) {
    super(lineno, detail);
    this.column = column;
    this.value = value;
  }

  /**
   * Fills the null or empty column values with fixed value.
   **
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Record} in which the 'col' value is lower cased.
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(column);
      if (idx == -1) {
        throw new StepException(toString() + " : Column '" + column + "' does not exist in the record.");
      }
      Object object = record.getValue(idx);
      if (object == null) {
        record.setValue(idx, value);
      } else {
        if (object instanceof String) {
          if (((String) object).isEmpty()) {
            record.setValue(idx, value);
          }
        } else {
          throw new StepException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.",
                          toString(), object != null ? object.getClass().getName() : "null", column)
          );
        }
      }
    }
    return records;
  }
}
