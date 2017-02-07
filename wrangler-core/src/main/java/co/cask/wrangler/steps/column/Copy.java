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

import java.util.List;

/**
 * A step to copy data from one column to another.
 */
public class Copy extends AbstractStep {
  private String source;
  private String destination;
  private boolean force;

  public Copy(int lineno, String detail, String source, String destination, boolean force) {
    super(lineno, detail);
    this.source = source;
    this.destination = destination;
    this.force = force;
  }

  /**
   * Copies data from one column to another.
   *
   * If the destination column doesn't exist then it will create one, else it will
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Record} in which the 'col' value is lower cased.
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int sidx = record.find(source);
      if (sidx == -1) {
        throw new StepException(toString() + " : Source column '" + source + "' does not exist in the record.");
      }

      int didx = record.find(destination);
      // If source and destination are same, then it's a nop.
      if (didx == sidx) {
        continue;
      }

      if (didx == -1) {
        // if destination column doesn't exist then add it.
        record.add(destination, record.getValue(sidx));
      } else {
        // if destination column exists, and force is set to false, then throw exception, else
        // overwrite it.
        if (!force) {
          throw new StepException(toString() + " : Destination column '" + destination
                                    + "' does not exist in the record. Use 'force' option to add new column.");
        }
        record.setValue(didx, record.getValue(sidx));
      }

    }
    return records;
  }
}
