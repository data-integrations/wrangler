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
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.StepException;
import org.unix4j.Unix4j;
import org.unix4j.builder.Unix4jCommandBuilder;

/**
 * A Wrangle step for 'Sed' like transformations on the column.
 */
public class Sed extends AbstractStep {
  private final String pattern;
  private final String column;

  public Sed(int lineno, String detail, String column, String pattern) {
    super(lineno, detail);
    this.pattern = pattern.trim();
    this.column = column;
  }

  /**
   * Sets the new column names for the {@link Row}.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Row}.
   * @throws StepException
   */
  @Override
  public Row execute(Row row, PipelineContext context) throws StepException, SkipRowException {
    Row r = new Row(row);
    int idx = row.find(column);
    if (idx != -1) {
      Object v = row.getValue(idx);
      // Operates only on String types.
      if (v instanceof String) {
        String value = (String) v; // Safely converts to String.
        Unix4jCommandBuilder builder = Unix4j.echo(value).sed(pattern);
        if (builder.toExitValue() == 0) {
          r.setValue(idx, builder.toStringResult());
        }
      }
    } else {
      throw new StepException(toString() + " : '" +
                                column + "' column is not defined. Please check the wrangling step."
      );
    }
    return r;
  }
}

