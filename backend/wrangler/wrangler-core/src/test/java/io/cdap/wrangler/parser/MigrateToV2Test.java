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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.GrammarMigrator;
import org.junit.Test;

/**
 * Tests {@link MigrateToV2}
 */
public class MigrateToV2Test {
  @Test
  public void testNullRecipe() throws Exception {
    GrammarMigrator migrator = new MigrateToV2((String) null);
    // no exception should be thrown.
    migrator.migrate();
  }

  @Test
  public void testEmptyRecipe() throws Exception {
    String recipe = "";
    GrammarMigrator migrator = new MigrateToV2(recipe);
    // no exception should be thrown.
    migrator.migrate();
  }

  @Test
  public void testCommentOnlyRecipe() throws Exception {
    String recipe = "// test";
    GrammarMigrator migrator = new MigrateToV2(recipe);
    // no exception should be thrown.
    migrator.migrate();
  }

  @Test
  public void testOldDirectivesWithNewSyntax() throws Exception {
    String recipe = "parse-as-csv :body '\t' true; drop :body;";
    GrammarMigrator migrator = new MigrateToV2(recipe);
    // no exception should be thrown.
    migrator.migrate();
  }
}
