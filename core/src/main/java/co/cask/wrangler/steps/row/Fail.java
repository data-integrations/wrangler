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

package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.steps.transformation.JexlHelper;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;

import java.util.List;

/**
 * A Wrangle step for erroring the processing if condition is set to true.
 */
@Usage(
  directive = "fail",
  usage = "fail <condition>",
  description = "Fails when the condition is evaluated to true."
)
public class Fail extends AbstractStep {
  private final String condition;
  private final JexlEngine engine;
  private final JexlScript script;

  public Fail(int lineno, String detail, String condition) {
    super(lineno, detail);
    this.condition = condition;
    // Create and build the script.
    engine = JexlHelper.getEngine();
    script = engine.createScript(condition);
  }

  /**
   * Fails the processing when conditions is set to true.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return the input {@link Record}, if condition is false
   * @throws StepException if there are any issues with processing the condition
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context)
    throws StepException {
    for (Record record : records) {
      // Move the fields from the record into the context.
      JexlContext ctx = new MapContext();
      ctx.set("this", record);
      for (int i = 0; i < record.length(); ++i) {
        ctx.set(record.getColumn(i), record.getValue(i));
      }

      // Execution of the script / expression based on the record data
      // mapped into context.
      try {
        boolean result = (Boolean) script.execute(ctx);
        if (result) {
          throw new StepException(
            String.format("Condition '%s' evaluated to true. Terminating processing.", condition)
          );
        }
      } catch (JexlException e) {
        // Generally JexlException wraps the original exception, so it's good idea
        // to check if there is a inner exception, if there is wrap it in 'StepException'
        // else just print the error message.
        if (e.getCause() != null) {
          throw new StepException(toString() + " : " + e.getMessage(), e.getCause());
        } else {
          throw new StepException(toString() + " : " + e.getMessage());
        }
      } catch (NumberFormatException e) {
        throw new StepException(toString() + " : " + " type mismatch. Change type of constant " +
                                  "or convert to right data type using conversion functions available. " +
                                  "Reason : " + e.getMessage());
      } catch (Exception e) {
        // We want to propogate this exception up!
        if (e instanceof ErrorRecordException) {
          throw e;
        }
        if (e.getCause() != null) {
          throw new StepException(toString() + " : " + e.getMessage(), e.getCause());
        } else {
          throw new StepException(toString() + " : " + e.getMessage());
        }
      }
    }
    return records;
  }
}
