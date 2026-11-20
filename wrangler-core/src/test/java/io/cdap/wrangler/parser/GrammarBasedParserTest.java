@@ -1,31 +1,35 @@
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
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.RecipeParser;
import org.apache.xbean.recipe.Recipe;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests {@link GrammarBasedParser}
 */
@@ -44,7 +48,7 @@ public void testBasic() throws Exception {

    RecipeParser parser = TestingRig.parse(recipe);
    List<Directive> directives = parser.parse();
    Assert.assertEquals(2, directives.size());
    assertEquals(2, directives.size());
  }

  @Test
@@ -61,7 +65,7 @@ public void testLoadableDirectives() throws Exception {

    Compiler compiler = new RecipeCompiler();
    CompileStatus status = compiler.compile(new MigrateToV2(recipe).migrate());
    Assert.assertEquals(7, status.getSymbols().getLoadableDirectives().size());
    assertEquals(7, status.getSymbols().getLoadableDirectives().size());
  }

  @Test
@@ -72,7 +76,18 @@ public void testCommentOnlyRecipe() throws Exception {

    RecipeParser parser = TestingRig.parse(recipe);
    List<Directive> directives = parser.parse();
    Assert.assertEquals(0, directives.size());
