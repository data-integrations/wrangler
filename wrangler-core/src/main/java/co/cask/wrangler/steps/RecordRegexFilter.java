/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import java.util.regex.Pattern;

/**
 * A Wrangle step for filtering rows that match the pattern specified on the column.
 */
public class RecordRegexFilter extends AbstractStep {
  private final String regex;
  private final String column;
  private Pattern pattern;

  public RecordRegexFilter(int lineno, String detail, String column, String regex) {
    super(lineno, detail);
    this.regex = regex.trim();
    this.column = column;
    pattern = Pattern.compile(this.regex);
  }

  /**
   * Sets the new column names for the {@link Record}.
   *
   * @param record Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Record}.
   * @throws StepException
   */
  @Override
  public Record execute(Record record, PipelineContext context) throws StepException, SkipRecordException {
    int idx = record.find(column);
    if (idx != -1) {
      String value = (String) record.getValue(idx);
      boolean status = pattern.matcher(value).matches(); // pattern.matcher(value).matches();
      if (status) {
        throw new SkipRecordException();
      }
    } else {
      throw new StepException(toString() + " : '" +
                                column + "' column is not defined. Please check the wrangling step."
      );
    }
    return record;
  }
}

