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
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.StepException;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Wrangle step for filtering rows based on the condition.
 *
 * <p>
 *   This step will evaluate the condition, if the condition evaluates to
 *   true, then the row will be skipped. If the condition evaluates to
 *   false, then the row will be accepted.
 * </p>
 */
public class RowConditionFilter extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(RowConditionFilter.class);
  private final String condition;
  private final JexlEngine engine;
  private final JexlScript script;

  public RowConditionFilter(int lineno, String detail, String condition) {
    super(lineno, detail);
    this.condition = condition;
    // Create and build the script.
    engine = new JexlBuilder().silent(false).cache(10).strict(true).create();
    script = engine.createScript(condition);
  }

  /**
   * Filters a row based on the condition.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return the input {@link Row}, if condition is false, else throw {@link SkipRowException}
   * @throws StepException if there are any issues with processing the condition
   * @throws SkipRowException if condition evaluates to true.
   */
  @Override
  public Row execute(Row row) throws StepException, SkipRowException {
    // Move the fields from the row into the context.
    JexlContext context = new MapContext();
    for (int i = 0; i < row.length(); ++i) {
      context.set(row.getColumn(i), row.getValue(i));
    }

    // Execution of the script / expression based on the row data
    // mapped into context.
    try {
      boolean result = (Boolean) script.execute(context);
      if (result) {
        throw new SkipRowException();
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
    }

    return row;
  }
}

