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

package co.cask.wrangler.steps.date;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * A Wrangle step for managing date formats.
 */
@Usage(
  directive = "format-date",
  usage = "format-date <column> <format>",
  description = "Formats a column using a date-time format. Use 'parse-as-date` beforehand."
)
public class FormatDate extends AbstractStep {
  private final String format;
  private final String column;
  private final DateFormat destinationFmt;

  public FormatDate(int lineno, String detail, String column, String format) {
    super(lineno, detail);
    this.column = column;
    this.format = format;
    this.destinationFmt = new SimpleDateFormat(this.format);
  }

  /**
   * Formats the date and sets the column.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Record}.
   * @throws StepException
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      Record dt = new Record(record);
      int idx = dt.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object != null && object instanceof Date) {
          dt.setValue(idx, destinationFmt.format((Date) object));
        } else {
          throw new StepException(
            String.format("%s : Invalid type '%s' of column '%s'. Apply 'parse-as-date' directive first.", toString(),
                          object != null ? object.getClass().getName() : "null", column)
          );
        }
      } else {
        throw new StepException(toString() + " : '" +
                                  column + "' column is not defined in the record. Please check the wrangling step."
        );
      }
      results.add(dt);
    }
    return results;
  }
}
