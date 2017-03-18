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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.AbstractIndependentStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangler step for title casing the 'col' value of type String.
 */
@Usage(
  directive = "titlecase",
  usage = "titlecase <column>",
  description = "Changes the column value to titlecase."
)
public class TitleCase extends AbstractIndependentStep {
  // Columns of the column to be lower cased.
  private String col;

  public TitleCase(int lineno, String detail, String col) {
    super(lineno, detail, col);
    this.col = col;
  }

  /**
   * Transforms a column value from any case to title case.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Record} in which the 'col' value is title cased.
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(col);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object instanceof String) {
          if (object != null) {
            String value = (String) object;
            record.setValue(idx, toTitleCase(value));
          }
        } else {
          throw new StepException(
            String.format("%s : Invalid value type '%s' of column '%s'. Should be of type String.",
                          toString(), object != null ? object.getClass().getName() : "null", col)
          );
        }
      } else {
        throw new StepException(toString() + " : " +
                                  col + " is not of type string. Please check the wrangle configuration."
        );
      }
      results.add(record);
    }
    return results;
  }

  private static String toTitleCase(String input) {
    StringBuilder titleCase = new StringBuilder();
    boolean nextTitleCase = true;

    for (char c : input.toCharArray()) {
      if (Character.isSpaceChar(c)) {
        nextTitleCase = true;
      } else if (nextTitleCase) {
        c = Character.toTitleCase(c);
        nextTitleCase = false;
      }

      titleCase.append(c);
    }
    return titleCase.toString();
  }
}
