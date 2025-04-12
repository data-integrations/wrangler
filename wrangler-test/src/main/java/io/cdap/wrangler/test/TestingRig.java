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

package io.cdap.wrangler.test;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.test.api.TestRecipe;

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
    return new RecipePipelineExecutor(parser, null);
  }

  public static RecipeParser parser(Class<? extends Directive> directive, String[] recipe)
    throws DirectiveParseException, DirectiveLoadException {
    verify(directive);
    List<String> packages = new ArrayList<>();
    packages.add(directive.getCanonicalName());
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      SystemDirectiveRegistry.INSTANCE
    );

    String migrate = new MigrateToV2(recipe).migrate();
    return new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
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
