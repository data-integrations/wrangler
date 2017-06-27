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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
import co.cask.wrangler.steps.transformation.JexlHelper;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangle step for filtering rows based on the condition.
 *
 * <p>
 *   This step will evaluate the condition, if the condition evaluates to
 *   true, then the row will be skipped. If the condition evaluates to
 *   false, then the row will be accepted.
 * </p>
 */
@Plugin(type = "directives")
@Name("filter-row-if-true")
@Usage("filter-row-if-true <condition>")
@Description("[DEPRECATED] Filters rows if condition is evaluated to true. Use 'filter-rows-on' instead.")
public class RecordConditionFilter extends AbstractDirective {
  private final String condition;
  private final JexlEngine engine;
  private final JexlScript script;
  private final boolean isTrue;

  public RecordConditionFilter(int lineno, String detail, String condition, boolean isTrue) {
    super(lineno, detail);
    this.condition = condition;
    this.isTrue = isTrue;
    // Create and build the script.
    engine = JexlHelper.getEngine();
    script = engine.createScript(condition);
  }

  /**
   * Filters a record based on the condition.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return the input {@link Row}, if condition is false
   * @throws DirectiveExecutionException if there are any issues with processing the condition
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      // Move the fields from the row into the context.
      JexlContext ctx = new MapContext();
      for (int i = 0; i < row.length(); ++i) {
        ctx.set(row.getColumn(i), row.getValue(i));
      }
      // Transient variables are added.
      if (context != null) {
        for (String variable : context.getTransientStore().getVariables()) {
          ctx.set(variable, context.getTransientStore().get(variable));
        }
      }

      // Execution of the script / expression based on the row data
      // mapped into context.
      try {
        boolean result = (Boolean) script.execute(ctx);
        if (!isTrue) {
          result = !result;
        }
        if (result) {
          continue;
        }
      } catch (JexlException e) {
        // Generally JexlException wraps the original exception, so it's good idea
        // to check if there is a inner exception, if there is wrap it in 'DirectiveExecutionException'
        // else just print the error message.
        if (e.getCause() != null) {
          throw new DirectiveExecutionException(toString() + " : " + e.getMessage(), e.getCause());
        } else {
          throw new DirectiveExecutionException(toString() + " : " + e.getMessage());
        }
      } catch (NumberFormatException e) {
        throw new DirectiveExecutionException(toString() + " : " + " type mismatch. Change type of constant " +
                                  "or convert to right data type using conversion functions available. Reason : " + e.getMessage());
      } catch (Exception e) {
        if (e.getCause() != null) {
          throw new DirectiveExecutionException(toString() + " : " + e.getMessage(), e.getCause());
        } else {
          throw new DirectiveExecutionException(toString() + " : " + e.getMessage());
        }
      }
      results.add(row);
    }
    return results;
  }
}
