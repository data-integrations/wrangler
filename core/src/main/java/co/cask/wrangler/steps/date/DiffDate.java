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

import co.cask.wrangler.api.AbstractSimpleStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Wrangle step for taking difference in Dates.
 */
@Usage(
  directive = "diff-date",
  usage = "diff-date <column> <column> <destination>",
  description = "Return the difference in milliseconds between two Date objects." +
    "Must use parse-simple-date or parse-date step first."
)
public class DiffDate extends AbstractSimpleStep {
  private final String column1;
  private final String column2;
  private final String destCol;
  private final Date date = new Date();

  public DiffDate(int lineno, String detail, String column1, String column2, String destCol) {
    super(lineno, detail);
    this.column1 = column1;
    this.column2 = column2;
    this.destCol = destCol;
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
    for (Record record : records) {
      Date date1 = getDate(record, column1);
      Date date2 = getDate(record, column2);
      if (date1 != null && date2 != null) {
        record.addOrSet(destCol, date1.getTime() - date2.getTime());
      } else {
        record.addOrSet(destCol, null);
      }
    }
    return records;
  }

  private Date getDate(Record record, String colName) throws StepException {
    // If one of the column contains now, then we return
    // the current date.
    if (colName.equalsIgnoreCase("now")) {
      return date;
    }

    // Else attempt to find the column.
    int idx = record.find(colName);
    if (idx == -1) {
      throw new StepException(toString() + " : '" +
                                colName + "' column is not defined in the record.");
    }
    Object o = record.getValue(idx);
    if (o == null || !(o instanceof Date)) {
      return null;
    }
    return (Date) o;
  }

  @Override
  public Map<String, Set<String>> getColumnMap() {
    return ImmutableMap.<String, Set<String>>of(destCol, ImmutableSet.of(column1, column2));
  }
}
