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
 * This class defines the interfaces that would provide the ability to
 * check whether the user should be provided access to the directive being used.
 */
public interface DirectiveEnforcer {

  /**
   * Checks if the directive is being excluded from being used.
   *
   * @param directive to be checked for exclusion.
   * @return true if excluded, false otherwise.
   */
  boolean isExcluded(String directive);
}
