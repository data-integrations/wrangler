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
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.StepException;

/**
 * Wrangle Step that merges two columns and creates a third column.
 */
public class Merge extends AbstractStep {
  // Source column1
  private String col1;

  // Source column2
  private String col2;

  // Destination column name to be created.
  private String dest;

  // Delimiter to be used to merge column.
  private String delimiter;

  public Merge(int lineno, String detail, String col1, String col2, String dest, String delimiter) {
    super(lineno, detail);
    this.col1 = col1;
    this.col2 = col2;
    this.dest = dest;
    this.delimiter = delimiter;
  }

  /**
   * Merges two columns using the delimiter into a third column.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A modified {@link Row} with merged column.
   * @throws StepException
   */
  @Override
  public Row execute(Row row, PipelineContext context) throws StepException, SkipRowException {
    int idx1 = row.find(col1);
    int idx2 = row.find(col2);
    if (idx1 != -1 && idx2 != -1) {
      StringBuilder builder = new StringBuilder();
      builder.append(row.getValue(idx1));
      builder.append(delimiter);
      builder.append(row.getValue(idx2));
      row.add(dest, builder.toString());
    }
    return row;
  }
}
