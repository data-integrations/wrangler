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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
import co.cask.wrangler.steps.transformation.JexlHelper;
import co.cask.wrangler.steps.transformation.functions.Types;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;

import java.util.List;

/**
 * Class description here.
 */
@Plugin(type = "udd")
@Name("increment-variable")
@Usage("increment-variable <variable> <value> <expression>")
@Description("Wrangler - A interactive tool for data cleansing and transformation.")
public class IncrementTransientVariable extends AbstractDirective {
  private final String variable;
  private final long incrementBy;
  private final String expression;
  private final JexlEngine engine;
  private final JexlScript script;

  public IncrementTransientVariable(int lineno, String detail, String variable, String value, String expression) {
    super(lineno, detail);
    this.variable = variable;
    this.expression = expression;
    engine = JexlHelper.getEngine();
    script = engine.createScript(this.expression);
    if (Types.isNumber(value)) {
      incrementBy = Long.parseLong(value);
    } else {
      incrementBy = 1;
    }
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows List of input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled List of {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context)
    throws DirectiveExecutionException, ErrorRowException {
    for (Row row : rows) {
      // Move the fields from the row into the context.
      JexlContext ctx = new MapContext();
      ctx.set("this", row);
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
        if (result) {
          context.getTransientStore().increment(variable, incrementBy);
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
        // We want to propogate this exception up!
        if (e instanceof ErrorRowException) {
          throw e;
        }
        if (e.getCause() != null) {
          throw new DirectiveExecutionException(toString() + " : " + e.getMessage(), e.getCause());
        } else {
          throw new DirectiveExecutionException(toString() + " : " + e.getMessage());
        }
      }
    }
    return rows;
  }
}
