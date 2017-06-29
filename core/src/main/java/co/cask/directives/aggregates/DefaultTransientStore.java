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

import co.cask.wrangler.api.TransientStore;

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
  private final Map<String, Object> variables = new HashMap<>();

  /**
   * Increments a value of the variable.
   *
   * @param name  of the variable.
   * @param value associated with the variable.
   */
  @Override
  public void increment(String name, long value) {
    Long count = get(name);
    if (count == null) {
      count = 0L;
    }
    set(name, count + value);
  }

  /**
   * Set of all the variables.
   *
   * @return list of all the variables.
   */
  @Override
  public Set<String> getVariables() {
    if(variables == null) {
      return new HashSet<>();
    }
    return variables.keySet();
  }

  /**
   * Resets the state of this store.
   */
  @Override
  public void reset() {
    variables.clear();
  }

  /**
   * A value associated with the variable in the transient store.
   *
   * @param name of the variable to be retrieved.
   * @return instance of object of type T.
   */
  @Override
  public <T> T get(String name) {
    return (T) variables.get(name);
  }

  /**
   * Sets the value of the object for variable named 'name'.
   *
   * @param name  of the variable for which the value needs to be set.
   * @param value of the variable.
   */
  @Override
  public void set(String name, Object value) {
    variables.put(name, value);
  }
}
