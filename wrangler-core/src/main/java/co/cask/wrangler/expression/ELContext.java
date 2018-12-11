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

package co.cask.wrangler.expression;

import co.cask.wrangler.api.ExecutorContext;
import org.apache.commons.jexl3.JexlContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages variables which can be referenced in a JEXL expression.
 *
 * <p>JEXL variable names in their simplest form are 'java-like' identifiers.
 * JEXL also considers 'ant' inspired variables expressions as valid.
 * For instance, the expression 'x.y.z' is an 'antish' variable and will be resolved as a whole by the context,
 * i.e. using the key "x.y.z". This proves to be useful to solve "fully qualified class names".</p>
 *
 * <p>Note that JEXL may use '$jexl' and '$ujexl' variables for internal purpose; setting or getting those
 * variables may lead to unexpected results unless specified otherwise.</p>
 */
public class ELContext implements JexlContext {
  private final Map<String, Object> values = new HashMap<>();

  /**
   * No-op constructors that does nothing but create a instance of context.
   */
  public ELContext() {
    // no-op
  }

  /**
   * Constructor that extracts the {@link ExecutorContext} internals and turns them into variables.
   * This method extracts the trasient variables, runtime arguments, environment it's running in and
   * the context in which it is running as identifiers that can be used within JEXL expression.
   *
   * @param context to be examined to be extracted into JEXL expression variables.
   */
  public ELContext(ExecutorContext context) {
    if (context == null) {
      return;
    }

    // Adds the transient store variables.
    for (String variable : context.getTransientStore().getVariables()) {
      values.put(variable, context.getTransientStore().get(variable));
    }

    values.put("runtime", context.getProperties());
    values.put("environment", context.getEnvironment().name());
    values.put("context", context.getContextName());
  }

  /**
   * This constructor sets the expression context with a variable.
   *
   * @param name of the variable.
   * @param object the object associated with the variable.
   */
  public ELContext(String name, Object object) {
    values.put(name, object);
  }

  /**
   * This constructor provides the pre-defined values for JEXL expression.
   *
   * @param values map of values.
   */
  public ELContext(Map<String, Object> values) {
    this.values.putAll(values);
  }

  /**
   * Returns the object associated with the name if found, else it's null.
   *
   * @param name of the variable.
   * @return value if found, null otherwise.
   */
  @Override
  public Object get(String name) {
    return values.get(name);
  }

  /**
   * Sets a variable with the value.
   *
   * @param name of the variable.
   * @param value of the variable.
   */
  @Override
  public void set(String name, Object value) {
    values.put(name, value);
  }

  /**d
   * Sets a variable with the value.
   *
   * @param name of the variable.
   * @param value of the variable.
   * @return 'this' context.
   */
  public ELContext add(String name, Object value) {
    values.put(name, value);
    return this;
  }

  /**
   * Checks if a variable exists in the context.
   *
   * @param name of the variable to be checked.
   * @return true if found, false otherwise.
   */
  @Override
  public boolean has(String name) {
    return values.containsKey(name);
  }
}
