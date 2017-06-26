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

import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.registry.DirectiveInfo;
import co.cask.wrangler.registry.DirectiveLoadException;
import co.cask.wrangler.registry.DirectiveLoader;
import co.cask.wrangler.registry.DirectiveNotFoundException;
import co.cask.wrangler.registry.DirectiveRegistry;
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
    String recipe = "rename :col1 :col2; parse-as-csv :body ',' true;";

    final DirectiveRegistry system = new SystemDirectiveRegistry();
    RecipeParser parser = new GrammarBasedParser(recipe, new DirectiveLoader() {
      @Override
      public UDD load(String name) throws DirectiveLoadException, DirectiveNotFoundException {
        DirectiveInfo info = system.get(name);
        if (info == null) {
          throw new DirectiveNotFoundException("Directive not found");
        }
        try {
          return info.instance();
        } catch (IllegalAccessException | InstantiationException e) {
          throw new DirectiveLoadException(e.getMessage(), e);
        }
      }
    });

    List<Directive> directives = parser.parse();
    Assert.assertEquals(2, directives.size());
  }
}