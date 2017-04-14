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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.AbstractIndependentStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * A Step to parse date into Date object.
 */
@Usage(
  directive = "parse-as-simple-date",
  usage = "parse-as-simple-date <column> <format>",
  description = "Parses a column as date using format."
)
public class ParseSimpleDate extends AbstractIndependentStep {
  private final String column;
  private final SimpleDateFormat format;

  public ParseSimpleDate(int lineno, String directive, String column, String format) {
    super(lineno, directive, column);
    this.column = column;
    this.format = new SimpleDateFormat(format);
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records  Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
        // If the data in the cell is null or is already of date format, then
        // continue to next record.
        if (object == null || object instanceof Date) {
          continue;
        }
        if (object instanceof String) {
          try {
            Date date = format.parse((String) object);
            record.setValue(idx, date);
          } catch (ParseException e) {
            throw new StepException(String.format("Failed to parse '%s' with pattern '%s'",
                                                  object, format.toPattern()));
          }
        } else {
          throw new StepException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          object != null ? object.getClass().getName() : "null", column)
          );
        }
      } else {
        throw new StepException(toString() + " : Column '" + column + "' does not exist in the record.");
      }
    }
    return records;
  }
}
