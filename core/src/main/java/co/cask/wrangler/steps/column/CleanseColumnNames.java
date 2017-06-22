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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.List;

/**
 * Cleanses columns names.
 *
 * <p>
 *   <ul>
 *     <li>Lowercases the column name</li>
 *     <li>Trims space</li>
 *     <li>Replace characters other than [A-Z][a-z][_] with empty string.</li>
 *   </ul>
 * </p>
 */

@Plugin(type = "udd")
@Name("cleanse-column-name")
@Usage("cleanse-column-names")
@Description("Sanatizes column names: trims, lowercases, and replaces all but [A-Z][a-z][0-9]_." +
  "with an underscore '_'.")
public class CleanseColumnNames extends AbstractStep {
  public CleanseColumnNames(int lineno, String directive) {
    super(lineno, directive);
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
        String column = record.getColumn(i);
        // Trims
        column = column.trim();
        // Lower case columns
        column = column.toLowerCase();
        // Filtering unwanted characters
        column = column.replaceAll("[^a-zA-Z0-9_]", "_");
        record.setColumn(i, column);
      }
    }
    return records;
  }
}
