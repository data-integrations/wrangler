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

package co.cask.directives.aggregates;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Expression;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.directives.JexlHelper;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;

import java.util.List;

/**
 * A directive that defines a transient variable who's life-expectancy is only within the record.
 *
 * The value set as transient variable is available to all the directives after that. But, it's
 * not available beyond the input record.
 */
@Plugin(type = Directive.Type)
@Name(SetTransientVariable.NAME)
@Description("Sets the value for a transient variable for the record being processed.")
public class SetTransientVariable implements Directive {
  public static final String NAME = "set-variable";
  private String variable;
  private String expression;
  private JexlEngine engine;
  private JexlScript script;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("variable", TokenType.IDENTIFIER);
    builder.define("condition", TokenType.EXPRESSION);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.variable = ((ColumnName) args.value("variable")).value();
    this.expression = ((Expression) args.value("condition")).value();
    engine = JexlHelper.getEngine();
    script = engine.createScript(this.expression);
  }

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
        Object result = script.execute(ctx);
        context.getTransientStore().set(variable, result);
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
