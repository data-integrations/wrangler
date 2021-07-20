/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.wrangler;

import io.cdap.wrangler.api.Row;
import org.apache.commons.jexl3.scripting.JexlScriptEngine;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import javax.script.SimpleScriptContext;

/**
 * A precondition expression that filters data into the directives.
 */
public class Precondition {
  private final String condition;
  private final CompiledScript script;
  // SimpleScriptContext is pretty expensive to construct due to all PrintWriter creation, so let's cache it
  private final ThreadLocal<ScriptContext> contextCache = ThreadLocal.withInitial(this::createContext);

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

  public ScriptContext createContext() {
    ScriptContext parent = script.getEngine().getContext();

    SimpleScriptContext context = new SimpleScriptContext();
    context.setBindings(parent.getBindings(ScriptContext.GLOBAL_SCOPE),
                         ScriptContext.GLOBAL_SCOPE);
    context.setWriter(parent.getWriter());
    context.setReader(parent.getReader());
    context.setErrorWriter(parent.getErrorWriter());
    return context;
  }

  public boolean apply(Row row) throws PreconditionException {
    Bindings bindings = new SimpleBindings();
    for (int i = 0; i < row.width(); ++i) {
      bindings.put(row.getColumn(i), row.getValue(i));
    }

    try {
      ScriptContext scriptContext = contextCache.get();
      scriptContext.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
      Object result = script.eval(scriptContext);
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
