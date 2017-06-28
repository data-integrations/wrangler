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

package co.cask.wrangler.parser;

import co.cask.wrangler.api.CompiledUnit;
import co.cask.wrangler.api.Compiler;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link GrammarBasedParser}
 */
public class GrammarBasedParserTest {

  @Test
  public void testBasic() throws Exception {
    String[] recipe = new String[] {
      "#pragma version 2.0",
      "#pragma load-directives text-reverse, text-exchange",
      "rename col1 col2",
      "parse-as-csv body , true"
    };

    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry()
    );

    RecipeParser parser = new GrammarBasedParser(new MigrateToV2(recipe).migrate(), registry);
    parser.initialize(null);
    List<Directive> directives = parser.parse();

    Assert.assertEquals(2, directives.size());
  }

  @Test
  public void testLoadableDirectives() throws Exception {
    String[] recipe = new String[] {
      "#pragma version 2.0",
      "#pragma load-directives text-reverse, text-exchange",
      "rename col1 col2",
      "parse-as-csv body , true",
      "!text-reverse :body;",
      "!test prop: { a='b', b=1.0, c=true};",
      "#pragma load-directives test-change,text-exchange, test1,test2,test3,test4"
    };

    Compiler compiler = new RecipeCompiler();
    CompiledUnit compiled = compiler.compile(new MigrateToV2(recipe).migrate());
    Assert.assertEquals(7, compiled.getLoadableDirectives().size());
  }
}