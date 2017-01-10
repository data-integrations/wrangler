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
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.StepException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A Wrangle step for managing date formats.
 *
 */
public class FormatDate extends AbstractStep {
  private final String source;
  private final String destination;
  private final String column;
  private final DateFormat sourceFmt;
  private final DateFormat destinationFmt;

  public FormatDate(int lineno, String detail, String column, String source, String destination) {
    super(lineno, detail);
    this.column = column;
    this.source = source;
    this.destination = destination;
    this.sourceFmt = new SimpleDateFormat(source);
    this.destinationFmt = new SimpleDateFormat(destination);
  }

  public FormatDate(int lineno, String detail, String column, String destination) {
    this(lineno, detail, column, "", destination);
  }

  /**
   * Formats the date and sets the column.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return A newly transformed {@link Row}.
   * @throws StepException
   */
  @Override
  public Row execute(Row row) throws StepException, SkipRowException {
    Row dt = new Row(row);
    int idx = dt.find(column);
    if (idx != -1) {
      try {
        Date date;
        String datetimestamp = (String) row.getValue(idx);
        if (datetimestamp.matches("[1-9][0-9]{9}")) {
          date = new Date(Long.parseLong(datetimestamp) * 1000);
        } else if (datetimestamp.matches("[1-9][0-9]{12}")) {
          date = new Date(Long.parseLong(datetimestamp));
        } else {
          date = sourceFmt.parse((String) row.getValue(idx));
        }
        dt.setValue(idx, destinationFmt.format(date));
      } catch (ParseException e) {
        throw new StepException(toString() + " : '" +
                                  column + ", " + e.getMessage(), e);
      }
    } else {
      throw new StepException(toString() + " : '" +
                                column + "' column is not defined. Please check the wrangling step."
      );
    }
    return dt;
  }
}

