/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.api;

/**
 * Interface for storing transient variables during directive execution.
 */
public interface TransientStore {
  /**
   * Gets a value from the store.
   *
   * @param scope The scope to get value from
   * @param name The name of the value
   * @return The stored value, or null if not found
   */
  Object get(TransientVariableScope scope, String name);

  /**
   * Sets a value in the store.
   *
   * @param scope The scope to store value in
   * @param name The name to store value under
   * @param value The value to store
   */
  void set(TransientVariableScope scope, String name, Object value);

  /**
   * Removes a value from the store.
   *
   * @param scope The scope to remove value from
   * @param name The name of value to remove
   */
  void remove(TransientVariableScope scope, String name);
}
