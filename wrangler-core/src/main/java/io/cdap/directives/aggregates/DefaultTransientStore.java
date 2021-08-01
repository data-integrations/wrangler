/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class implements a transient store interface for storing variables
 * that can be set within a step and are available across all the steps when
 * the record is being processed.
 *
 * The life-time of variables set in this store is within a boundary of record
 * being processed.
 */
public class DefaultTransientStore implements TransientStore {
  private final Map<String, Object> global = new HashMap<>();
  private final Map<String, Object> local = new HashMap<>();

  /**
   * Increments a value of the variable.
   *
   * @param name  of the variable.
   * @param value associated with the variable.
   */
  @Override
  public void increment(TransientVariableScope scope, String name, long value) {
    if (scope == TransientVariableScope.GLOBAL) {
      increment(global, name, value);
    } else if (scope == TransientVariableScope.LOCAL) {
      increment(local, name, value);
    }
  }

  private void increment(Map<String, Object> variables, String name, long value) {
    Long count = null;
    if (variables.containsKey(name)) {
      count = (Long) variables.get(name);
    }
    if (count == null) {
      count = 0L;
    }
    variables.put(name, count + value);
  }

  /**
   * Set of all the variables.
   *
   * @return list of all the variables.
   */
  @Override
  public Set<String> getVariables() {
    Set<String> vars = new HashSet<>(global.keySet());
    vars.addAll(local.keySet());
    return vars;
  }

  /**
   * Resets the state of this store.
   */
  @Override
  public void reset(TransientVariableScope scope) {
    if (scope == TransientVariableScope.GLOBAL) {
      global.clear();
    } else if (scope == TransientVariableScope.LOCAL) {
      local.clear();
    }
  }

  /**
   * A value associated with the variable in the transient store.
   *
   * @param name of the variable to be retrieved.
   * @return instance of object of type T.
   */
  @Override
  public <T> T get(String name) {
    if (global.containsKey(name)) {
      return (T) global.get(name);
    }
    return (T) local.get(name);
  }

  /**
   * Sets the value of the object for variable named 'name'.
   *
   * @param name  of the variable for which the value needs to be set.
   * @param value of the variable.
   */
  @Override
  public void set(TransientVariableScope scope, String name, Object value) {
    if (scope == TransientVariableScope.GLOBAL) {
      global.put(name, value);
    } else if (scope == TransientVariableScope.LOCAL) {
      local.put(name, value);
    }
  }
}
