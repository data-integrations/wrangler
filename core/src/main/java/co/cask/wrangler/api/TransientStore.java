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

package co.cask.wrangler.api;

import java.util.Set;

/**
 * {@link TransientStore} is an interface that holds volatile information that's
 * present across all the steps associated with the directives that are processing
 * a single record.
 */
public interface TransientStore {
  /**
   * Resets the state of this store.
   */
  void reset();

  /**
   * A value associated with the variable in the transient store.
   *
   * @param name of the variable to be retrieved.
   * @param <T> type of the value to be returned.
   * @return instance of object of type T. return null if variable doesn't exist.
   */
  <T> T get(String name);

  /**
   * Sets the value of the object for variable named 'name'.
   *
   * @param name of the variable for which the value needs to be set.
   * @param value of the variable.
   */
  void set(String name, Object value);

  /**
   * Increments a value of the variable.
   *
   * @param name of the variable.
   * @param value associated with the variable.
   */
  void increment(String name, long value);

  /**
   * Set of all the variables.
   *
   * @return list of all the variables.
   */
  Set<String> getVariables();

  /**
   * Delete a variable
   *
   * @param name
   * @return previous value of this variable, or null if it doesn't exist
   */
  <T> T delete(String name);
}
