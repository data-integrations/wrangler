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

import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.expression.CompileOptions;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELPermissionException;
import io.cdap.wrangler.parser.NoOpDirectiveContext;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.introspection.JexlSandbox;

import java.util.Objects;

/**
 * A precondition expression that filters data into the directives.
 */
public class Precondition {
  private final String condition;
  private final JexlScript script;
  private final boolean allowlistEnabled;

  /**
   * Compiles a JEXL precondition expression without allowlist enforcement.
   * Intended for compile-time syntax validation where runtime directive config is unavailable.
   *
   * @param condition the JEXL precondition expression
   * @throws PreconditionException if compilation fails
   */
  public Precondition(String condition) throws PreconditionException {
    this(condition, new NoOpDirectiveContext());
  }

  /**
   * Compiles a JEXL precondition expression using the JEXL allowlist configuration from the given context.
   *
   * @param condition the JEXL precondition expression
   * @param context   the directive context holding JEXL allowlist configuration
   * @throws PreconditionException if compilation fails
   * @throws ELPermissionException if compilation violates JEXL sandbox permissions
   */
  public Precondition(String condition, DirectiveContext context) throws PreconditionException {
    Objects.requireNonNull(condition, "Precondition expression must not be null.");
    this.condition = condition;
    CompileOptions options = CompileOptions.fromContext(context);
    this.allowlistEnabled = options.isAllowlistEnabled();
    JexlSandbox sandbox = EL.createSandbox(options.getJexlAllowlist(), allowlistEnabled);
    JexlEngine engine = new JexlBuilder()
      .sandbox(sandbox)
      .silent(false)
      .cache(1024)
      .strict(true)
      .create();
    try {
      script = engine.createScript(condition);
    } catch (JexlException e) {
      EL.checkJexlPermissionViolation(e, condition, allowlistEnabled);
      throw toPreconditionException(e);
    } catch (RuntimeException e) {
      throw toPreconditionException(e);
    }
  }

  /**
   * Evaluates the compiled JEXL precondition expression against the given row.
   *
   * @param row the row providing column bindings for the precondition expression
   * @return {@code true} if the row satisfies the precondition, {@code false} otherwise
   * @throws PreconditionException if evaluation fails or does not produce a boolean result
   * @throws ELPermissionException if evaluation violates JEXL sandbox permissions
   */
  public boolean apply(Row row) throws PreconditionException {
    JexlContext jexlContext = new MapContext();
    for (int i = 0; i < row.width(); ++i) {
      jexlContext.set(row.getColumn(i), row.getValue(i));
    }

    Object result;
    try {
      result = script.execute(jexlContext);
    } catch (JexlException e) {
      EL.checkJexlPermissionViolation(e, condition, allowlistEnabled);
      throw toPreconditionException(e);
    } catch (RuntimeException e) {
      throw toPreconditionException(e);
    }

    if (!(result instanceof Boolean)) {
      throw new PreconditionException(
        String.format("Precondition '%s' does not result in true or false.", condition)
      );
    }
    return (Boolean) result;
  }

  private static PreconditionException toPreconditionException(RuntimeException e) {
    // Generally JexlException wraps the original exception, so check if there is an inner exception.
    if (e.getCause() != null && e.getCause().getMessage() != null) {
      return new PreconditionException(e.getCause().getMessage());
    }
    return new PreconditionException(e.getMessage() != null ? e.getMessage() : e.toString());
  }
}
