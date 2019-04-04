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
 * This interface {@link DirectiveAlias} provides a way to check if a directive
 * is aliased and if there it is aliased to what it is being aliased.
 */
public interface DirectiveAlias {

  /**
   * Checks if the directive is aliased.
   *
   * @param directive to be checked for aliasing.
   * @return true if the directive has an alias, false otherwise.
   */
  boolean hasAlias(String directive);

  /**
   * Returns the root directive aliasee
   * @param directive
   * @return
   */
  String getAlias(String directive);
}
