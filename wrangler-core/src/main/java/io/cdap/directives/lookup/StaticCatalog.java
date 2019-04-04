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

package io.cdap.directives.lookup;

import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;

/**
 * Interface for defining different kinds of catalog.
 */
@PublicEvolving
public interface StaticCatalog extends Serializable {
  /**
   * Configures a catalog.
   *
   * @return true if success in configuring, false otherwise.
   */
  boolean configure();

  /**
   * Looks up the code in the catalog.
   *
   * @param code to be looked up.
   * @return StaticCatalog entry if found, else null.
   */
  StaticCatalog.Entry lookup(String code);

  /**
   * @return name of the catalog.
   */
  String getCatalog();

  /**
   * An entry in the catalog.
   */
  interface Entry {
    String getDescription();
  }
}
