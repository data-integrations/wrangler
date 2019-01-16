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

package co.cask.wrangler.parser;

import co.cask.wrangler.api.DirectiveConfig;
import co.cask.wrangler.api.DirectiveNotFoundException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Executor;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.proto.Contexts;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link NoOpDirectiveContext}
 */
public class ConfigDirectiveContextTest {

  private static final String CONFIG = "{\n" +
    "\t\"exclusions\" : [\n" +
    "\t\t\"parse-as-csv\",\n" +
    "\t\t\"parse-as-excel\",\n" +
    "\t\t\"set\",\n" +
    "\t\t\"invoke-http\",\n" +
    "\t\t\"js-parser\"\n" +
    "\t],\n" +
    "\n" +
    "\t\"aliases\" : {\n" +
    "\t\t\"json-parser\" : \"parse-as-json\",\n" +
    "\t\t\"js-parser\" : \"parse-as-json\"\n" +
    "\t}\n" +
    "}";

  private static final String EMPTY = "{}";

  @Test(expected = DirectiveParseException.class)
  public void testBasicExclude() throws Exception {
    String[] text = new String[] {
      "parse-as-csv body , true"
    };

    Gson gson = new Gson();
    DirectiveConfig config = gson.fromJson(CONFIG, DirectiveConfig.class);

    RecipeParser directives = new GrammarBasedParser(Contexts.SYSTEM, text,
                                                     new CompositeDirectiveRegistry(new SystemDirectiveRegistry())
    );
    directives.initialize(new ConfigDirectiveContext(config));
    directives.parse();
  }

  @Test(expected = DirectiveParseException.class)
  public void testAliasedAndExcluded() throws Exception {
    String[] text = new String[] {
      "js-parser body"
    };

    Gson gson = new Gson();
    DirectiveConfig config = gson.fromJson(CONFIG, DirectiveConfig.class);

    RecipeParser directives = new GrammarBasedParser(Contexts.SYSTEM, text,
                                                     new CompositeDirectiveRegistry(new SystemDirectiveRegistry())
    );
    directives.initialize(new ConfigDirectiveContext(config));
    directives.parse();
  }

  @Test
  public void testAliasing() throws Exception {
    String[] text = new String[] {
      "json-parser :body;"
    };

    Gson gson = new Gson();
    DirectiveConfig config = gson.fromJson(CONFIG, DirectiveConfig.class);

    RecipeParser directives = new GrammarBasedParser(Contexts.SYSTEM, text,
                                                     new CompositeDirectiveRegistry(new SystemDirectiveRegistry())
    );
    directives.initialize(new ConfigDirectiveContext(config));

    List<Executor> steps = directives.parse();
    Assert.assertEquals(1, steps.size());
  }

  @Test(expected = DirectiveNotFoundException.class)
  public void testEmptyAliasingShouldFail() throws Exception {
    String[] text = new String[] {
      "json-parser :body;"
    };

    Gson gson = new Gson();
    DirectiveConfig config = gson.fromJson(EMPTY, DirectiveConfig.class);

    RecipeParser directives = new GrammarBasedParser(Contexts.SYSTEM, text,
                                                     new CompositeDirectiveRegistry(new SystemDirectiveRegistry())
    );
    directives.initialize(new ConfigDirectiveContext(config));

    List<Executor> steps = directives.parse();
    Assert.assertEquals(1, steps.size());
  }

  @Test
  public void testWithNoAliasingNoExclusion() throws Exception {
    String[] text = new String[] {
      "parse-as-json :body;"
    };

    Gson gson = new Gson();
    DirectiveConfig config = gson.fromJson(EMPTY, DirectiveConfig.class);

    RecipeParser directives = new GrammarBasedParser(Contexts.SYSTEM, text,
                                                     new CompositeDirectiveRegistry(new SystemDirectiveRegistry())
    );
    directives.initialize(new ConfigDirectiveContext(config));

    List<Executor> steps = directives.parse();
    Assert.assertEquals(1, steps.size());
  }

}
