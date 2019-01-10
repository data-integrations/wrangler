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

package co.cask.wrangler.test;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.RecipeException;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.proto.Contexts;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import co.cask.wrangler.test.api.TestRecipe;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for testing.
 */
public final class TestingRig {

  private TestingRig() {
    // Avoid creation of this object.
  }

  public static RecipePipeline pipeline(Class<? extends Directive> directive, TestRecipe recipe)
    throws RecipeException, DirectiveParseException, DirectiveLoadException {
    verify(directive);
    List<String> packages = new ArrayList<>();
    packages.add(directive.getPackage().getName());
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry(packages)
    );

    String migrate = new MigrateToV2(recipe.toArray()).migrate();
    RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
    parser.initialize(null);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.initialize(parser, null);
    return pipeline;
  }

  public static RecipeParser parser(Class<? extends Directive> directive, String[] recipe)
    throws DirectiveParseException, DirectiveLoadException {
    verify(directive);
    List<String> packages = new ArrayList<>();
    packages.add(directive.getCanonicalName());
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry()
    );

    String migrate = new MigrateToV2(recipe).migrate();
    RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
    parser.initialize(null);
    return parser;
  }

  private static void verify(Class<? extends Directive> directive) {
    String classz = directive.getCanonicalName();
    Plugin plugin = directive.getAnnotation(Plugin.class);
    if (plugin == null || !plugin.type().equalsIgnoreCase(Directive.TYPE)) {
      throw new IllegalArgumentException(
        String.format("Class '%s' @Plugin annotation is not of type '%s', Set it as @Plugin(type=UDD.Type)",
                      classz, Directive.TYPE)
      );
    }

    Name name = directive.getAnnotation(Name.class);
    if (name == null) {
      throw new IllegalArgumentException(
        String.format("Class '%s' is missing @Name annotation. E.g. @Name(\"directive-name\")", classz)
      );
    }

    Description description = directive.getAnnotation(Description.class);
    if (description == null) {
      throw new IllegalArgumentException(
        String.format("Class '%s' is missing @Description annotation. " +
                        "E.g. @Description(\"this is what my directive does\")", classz)
      );
    }
  }

}
