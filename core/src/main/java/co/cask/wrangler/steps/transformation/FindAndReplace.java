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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import org.unix4j.Unix4j;
import org.unix4j.builder.Unix4jCommandBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangle step for 'find-and-replace' transformations on the column.
 */
@Plugin(type = "udd")
@Name("find-and-replace")
@Usage("find-and-replace <column> <sed-expression>")
@Description("Finds and replaces text in column values using a sed-format expression")
public class FindAndReplace extends AbstractStep {
  private final String pattern;
  private final String column;

  public FindAndReplace(int lineno, String detail, String column, String pattern) {
    super(lineno, detail);
    this.pattern = pattern.trim();
    this.column = column;
  }

  /**
   * Sets the new column names for the {@link Record}.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Record}.
   * @throws StepException throw when there is issue executing the grep.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object v = record.getValue(idx);
        // Operates only on String types.
        try {
          if (v instanceof String) {
            String value = (String) v; // Safely converts to String.
            Unix4jCommandBuilder builder = Unix4j.echo(value).sed(pattern);
            if (builder.toExitValue() == 0) {
              record.setValue(idx, builder.toStringResult());
            }
          }
        } catch (Exception e) {
          // If there is any issue, we pass it on without any transformation.
        }
      } else {
        throw new StepException(toString() + " : '" +
                                  column + "' column is not defined. Please check the wrangling step."
        );
      }
      results.add(record);
    }
    return results;
  }
}

