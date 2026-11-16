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

import com.google.common.base.Strings;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELPermissionException;
import io.cdap.wrangler.expression.ELResult;

import javax.annotation.Nullable;

/**
 * A precondition expression that filters data into the directives.
 */
public class Precondition {
  private final String condition;
  private final EL script;

  public Precondition(String condition) throws PreconditionException {
    this(condition, null);
  }

  public Precondition(String condition, @Nullable DirectiveContext directiveContext)
      throws PreconditionException {
    this.condition = condition;
    if (Strings.isNullOrEmpty(condition) || condition.trim().isEmpty()) {
      this.script = null;
      return;
    }
    if (directiveContext != null) {
      EL.initialize(directiveContext);
    }
    try {
      this.script = EL.compile(condition);
    } catch (ELException | ELPermissionException e) {
      throw new PreconditionException(e.getMessage(), e);
    } finally {
      if (directiveContext != null) {
        EL.reset();
      }
    }
  }

  public boolean apply(Row row) throws PreconditionException {
    if (script == null) {
      return false;
    }
    ELContext context = new ELContext();
    for (int i = 0; i < row.width(); ++i) {
      context.set(row.getColumn(i), row.getValue(i));
    }
    context.set("this", row);

    try {
      ELResult elResult = script.execute(context);
      Object result = elResult.getObject();
      if (!(result instanceof Boolean)) {
        throw new PreconditionException(
          String.format("Precondition '%s' does not result in true or false.", condition)
        );
      }
      return (Boolean) result;
    } catch (ELException | ELPermissionException e) {
      throw new PreconditionException(e.getMessage(), e);
    }
  }
}
