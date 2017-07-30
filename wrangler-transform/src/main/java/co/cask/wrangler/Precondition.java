/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler;

import co.cask.wrangler.api.Row;
import org.apache.commons.jexl3.scripting.JexlScriptEngine;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

/**
 * A precondition expression that filters data into the directives.
 */
public class Precondition {
  private final String condition;
  private final CompiledScript script;

  public Precondition(String condition) throws PreconditionException {
    this.condition = condition;
    JexlScriptEngine engine = new JexlScriptEngine();
    try {
      script = engine.compile(condition);
    } catch (ScriptException e) {
      if (e.getCause() != null) {
        throw new PreconditionException(e.getCause().getMessage());
      } else {
        throw new PreconditionException(e.getMessage());
      }
    }
  }

  public boolean apply(Row row) throws PreconditionException {
    Bindings ctx = new SimpleBindings();
    for (int i = 0; i < row.length(); ++i) {
      ctx.put(row.getColumn(i), row.getValue(i));
    }

    try {
      Object result = script.eval(ctx);
      if (!(result instanceof Boolean)) {
        throw new PreconditionException(
          String.format("Precondition '%s' does not result in true or false.", condition)
        );
      }
      return (Boolean) result;
    } catch (ScriptException e) {
      // Generally JexlException wraps the original exception, so it's good idea
      // to check if there is a inner exception, if there is wrap it in 'DirectiveExecutionException'
      // else just print the error message.
      if (e.getCause() != null) {
        throw new PreconditionException(e.getCause().getMessage());
      } else {
        throw new PreconditionException(e.getMessage());
      }
    }
  }
}
