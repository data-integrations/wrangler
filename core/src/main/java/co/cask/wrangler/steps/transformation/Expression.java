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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.Usage;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Wrangler step for apply an expression to store the result in a column.
 *
 * The expressions are specified in JEXL format (http://commons.apache.org/proper/commons-jexl/reference/syntax.html)
 * Directive is response for executing only one expression for each {@link Record} record that is
 * being passed. The result of expression either adds a new column or replaces the value of
 * the existing column.
 *
 * <p>
 *   Directive step = new Expression(lineno, directive, column, "if (age > 24 ) { 'adult' } else { 'teen' }");
 * </p>
 */
@Plugin(type = "udd")
@Name("set column")
@Usage("set column <column> <jexl-expression>")
@Description("Sets a column by evaluating a JEXL expression.")
public class Expression extends AbstractStep {
  // Column to which the result of experience is applied to.
  private final String column;

  // The actual expression
  private final String expression;

  // Handler to Jexl Engine.
  private final JexlEngine engine;

  // Parsed / Compiled expression.
  private final JexlScript script;

  // Properties associated with pipeline
  private final Map<String, Object> properties = new HashMap<>();


  public Expression(int lineno, String detail, String column, String expression) {
    super(lineno, detail);
    this.column = column;
    this.expression = expression;
    // Create and build the script.
    engine = JexlHelper.getEngine();
    script = engine.createScript(expression);
  }

  /**
   * Transforms a column value from any case to upper case.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Record} in which the 'col' value is lower cased.
   * @throws DirectiveExecutionException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws DirectiveExecutionException {
    // This is done only the first time.
    if (properties.size() == 0 && context != null) {
      properties.putAll(context.getProperties());
    }

    for (Record record : records) {
      // Move the fields from the record into the context.
      JexlContext ctx = new MapContext(properties);
      ctx.set("this", record);
      for (int i = 0; i < record.length(); ++i) {
        ctx.set(record.getColumn(i), record.getValue(i));
      }
      // Transient variables are added.
      if (context != null) {
        for (String variable : context.getTransientStore().getVariables()) {
          ctx.set(variable, context.getTransientStore().get(variable));
        }
      }

      // Execution of the script / expression based on the record data
      // mapped into context.
      try {
        Object result = script.execute(ctx);
        int idx = record.find(this.column);
        if (idx == -1) {
          record.add(this.column, result);
        } else {
          record.setValue(idx, result);
        }
      } catch (JexlException e) {
        // Generally JexlException wraps the original exception, so it's good idea
        // to check if there is a inner exception, if there is wrap it in 'DirectiveExecutionException'
        // else just print the error message.
        if (e.getCause() != null) {
          throw new DirectiveExecutionException(toString() + " : " + e.getCause().getMessage());
        } else {
          throw new DirectiveExecutionException(toString() + " : " + e.getMessage());
        }
      }
    }
    return records;
  }
}


