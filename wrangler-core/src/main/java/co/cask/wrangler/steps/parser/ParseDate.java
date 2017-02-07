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

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * A Step to parse date.
 */
public class ParseDate extends AbstractStep {
  private final String column;
  private final String timezone;

  public ParseDate(int lineno, String directive, String column, String timezone) {
    super(lineno, directive);
    this.column = column;
    this.timezone = timezone;
    if(timezone == null) {
      timezone = "UTC";
    }
    TimeZone.setDefault(TimeZone.getTimeZone(timezone));
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
        if (object instanceof String) {
          Parser parser = new Parser();
          List<DateGroup> groups = parser.parse((String) object);
          int i = 1;
          for (DateGroup group : groups) {
            List<Date> dates = group.getDates();
            for (Date date : dates) {
              record.add(String.format("%s_%d", column, i), date);
            }
            i++;
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
