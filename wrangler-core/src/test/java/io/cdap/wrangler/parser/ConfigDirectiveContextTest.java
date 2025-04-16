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

package io.cdap.wrangler.parser;

import com.google.gson.Gson;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
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

  @Test(expected = RecipeException.class)
  public void testBasicExclude() throws Exception {
    String[] text = new String[] {
      "parse-as-csv body , true"
    };

    Gson gson = new Gson();
    DirectiveConfig config = gson.fromJson(CONFIG, DirectiveConfig.class);

    RecipeParser directives = new GrammarBasedParser(Contexts.SYSTEM, text,
                                                     new CompositeDirectiveRegistry(SystemDirectiveRegistry.INSTANCE),
                                                     new ConfigDirectiveContext(config));
    directives.parse();
  }

  @Test(expected = RecipeException.class)
  public void testAliasedAndExcluded() throws Exception {
    String[] text = new String[] {
      "js-parser body"
    };

    Gson gson = new Gson();
    DirectiveConfig config = gson.fromJson(CONFIG, DirectiveConfig.class);

    RecipeParser directives = new GrammarBasedParser(Contexts.SYSTEM, text,
                                                     new CompositeDirectiveRegistry(SystemDirectiveRegistry.INSTANCE),
                                                     new ConfigDirectiveContext(config));
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
                                                     new CompositeDirectiveRegistry(SystemDirectiveRegistry.INSTANCE),
                                                     new ConfigDirectiveContext(config));
    List<Directive> steps = directives.parse();
    Assert.assertEquals(1, steps.size());
  }

  @Test(expected = RecipeException.class)
  public void testEmptyAliasingShouldFail() throws Exception {
    String[] text = new String[] {
      "json-parser :body;"
    };

    Gson gson = new Gson();
    DirectiveConfig config = gson.fromJson(EMPTY, DirectiveConfig.class);

    RecipeParser directives = new GrammarBasedParser(Contexts.SYSTEM, text,
                                                     new CompositeDirectiveRegistry(SystemDirectiveRegistry.INSTANCE),
                                                     new ConfigDirectiveContext(config));
    List<Directive> steps = directives.parse();
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
                                                     new CompositeDirectiveRegistry(SystemDirectiveRegistry.INSTANCE),
                                                     new ConfigDirectiveContext(config));
    List<Directive> steps = directives.parse();
    Assert.assertEquals(1, steps.size());
  }

}
