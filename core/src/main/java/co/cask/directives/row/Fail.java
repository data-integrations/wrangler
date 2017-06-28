/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.directives.row;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.parser.Expression;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.steps.transformation.JexlHelper;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;

import java.util.List;

/**
 * A directive for erroring the processing if condition is set to true.
 */
@Plugin(type = UDD.Type)
@Name(Fail.DIRECTIVE_NAME)
@Description("Fails when the condition is evaluated to true.")
public class Fail implements UDD {
  public static final String DIRECTIVE_NAME = "fail";
  private String condition;
  private JexlEngine engine;
  private JexlScript script;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(DIRECTIVE_NAME);
    builder.define("condition", TokenType.EXPRESSION);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    Expression expression = args.value("condition");
    if(expression.value().isEmpty()) {
      throw new DirectiveParseException(
        String.format("No condition has been specified.")
      );
    }
    condition = expression.value();
    engine = JexlHelper.getEngine();
    script = engine.createScript(condition);
  }

  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context)
    throws DirectiveExecutionException {
    for (Row row : rows) {
      // Move the fields from the row into the context.
      JexlContext ctx = new MapContext();
      ctx.set("this", row);
      for (int i = 0; i < row.length(); ++i) {
        ctx.set(row.getColumn(i), row.getValue(i));
      }

      // Execution of the script / expression based on the row data
      // mapped into context.
      try {
        boolean result = (Boolean) script.execute(ctx);
        if (result) {
          throw new DirectiveExecutionException(
            String.format("Condition '%s' evaluated to true. Terminating processing.", condition)
          );
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
                                  "or convert to right data type using conversion functions available. " +
                                  "Reason : " + e.getMessage());
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
