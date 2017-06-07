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

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.List;

/**
 * A Wrangler step for converting data type of a column
 * Accepted types are: int, double, string and boolean
 */
@Usage(
        directive = "set-type",
        usage = "set-type <column> <int|double|string|boolean>",
        description = "Converting data type of a column"
)
public class SetType extends AbstractStep {
  // Columns of the column to be upper-cased
  private String col;
  private String type;

  public SetType(int lineno, String detail, String col, String type) {
    super(lineno, detail);
    this.col = col;
    this.type = type;
  }

  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(col);
      if (idx != -1) {
        Object object = record.getValue(idx);

        //convert to int
        if (type.equals("int")) {
          if (object instanceof String) {
            if (object != null) {
              String value = (String) object;
              record.setValue(idx, Integer.valueOf(value));
            }
          }
          else if (object instanceof Double) {
            if (object != null) {
              Double value = (Double) object;
              record.setValue(idx, value.intValue());
            }
          }
          else if (object instanceof Boolean) {
            if (object != null) {
              Boolean value = (Boolean) object;
              if (value.equals(true)) {
                record.setValue(idx, 1);
              }
              else {
                record.setValue(idx, 0);
              }
            }
          }
        }

        //convert to string
        if (type.equals("string")) {
          if (object != null) {
            record.setValue(idx, object.toString());
          }
        }

        //convert to double
        if (type.equals("double")) {
          if (object instanceof Integer) {
            if (object != null) {
              Integer value = (Integer) object;
              record.setValue(idx, value.doubleValue());
            }
          }
          if (object instanceof String) {
            if (object != null) {
              String value = (String) object;
              record.setValue(idx, Double.valueOf(value));
            }
          }
          if (object instanceof Boolean) {
            if (object != null) {
              Boolean value = (Boolean) object;
              if (value.equals(true)) {
                record.setValue(idx, 1.0);
              }
              else {
                record.setValue(idx, 0.0);
              }
            }
          }
        }

        //convert to boolean
        if (type.equals("boolean")) {
          if (object instanceof Integer) {
            if (object != null) {
              Integer value = (Integer) object;
              if (value.equals(0)) {
                record.setValue(idx, false);
              }
              else {
                record.setValue(idx, true);
              }
            }
          }
          if (object instanceof Double) {
            if (object != null) {
              Double value = (Double) object;
              if (value.equals(0.0)) {
                record.setValue(idx, false);
              }
              else {
                record.setValue(idx, true);
              }
            }
          }
          if (object instanceof String) {
            if (object != null) {
              String value = (String) object;
              if (value.equals("true")) {
                record.setValue(idx, true);
              }
              else if (value.equals("false")) {
                record.setValue(idx, false);
              }
            }
          }
        }
      }
    }
    return records;
  }
}
