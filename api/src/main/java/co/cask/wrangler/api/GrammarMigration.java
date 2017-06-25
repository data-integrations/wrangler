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

package co.cask.wrangler.api;

import java.util.List;

/**
 * This is a interface for migrating grammar from one version to other.
 */
public interface GrammarMigration {

  /**
   * Checks to see if directive is migratable.
   *
   * @param directives to be checked if it's migratable.
   * @return true if the directives are migrateable, false otherwise.
   */
  boolean isMigrateable(List<String> directives);

  /**
   * Migrates each directive from one version to other.
   *
   * @param directives to be migrated to newer version.
   * @return directives transformed into a newer version.
   */
  List<String> migrate(List<String> directives) throws DirectiveParseException;
}
