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
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * A Step to split email address into account and domain.
 */
public class SplitEmail extends AbstractStep {
  private final String column;

  public SplitEmail(int lineno, String directive, String column) {
    super(lineno, directive);
    this.column = column;
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
        if (object == null) {
          record.add(column + ".account", null);
          record.add(column + ".domain", null);
          continue;
        }
        if (object instanceof String) {
          String[] parts = ((String) object).split("@");
          if (parts.length > 1) {
            record.add(column + ".account", parts[0]);
            record.add(column + ".domain", StringUtils.join(ArrayUtils.subarray(parts, 1, parts.length), "@"));
          } else if (parts.length == 1) {
            record.add(column + ".account", parts[0]);
            record.add(column + ".domain", null);
          } else {
            record.add(column + ".account", null);
            record.add(column + ".domain", null);
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
