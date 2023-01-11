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

import com.google.common.base.Joiner;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveNotFoundException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.DirectiveRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class <code>GrammarBasedParser</code> is an implementation of <code>RecipeParser</code>.
 * It's responsible for compiling the recipe and checking all the directives exist before concluding
 * that the directives are ready for execution.
 */
public class GrammarBasedParser implements RecipeParser {
  private static final char EOL = '\n';
  private final String namespace;
  private final DirectiveRegistry registry;
  private final String recipe;
  private final DirectiveContext context;

  public GrammarBasedParser(String namespace, String recipe, DirectiveRegistry registry) {
    this(namespace, recipe, registry, new NoOpDirectiveContext());
  }

  public GrammarBasedParser(String namespace, String[] directives,
                            DirectiveRegistry registry, DirectiveContext context) {
    this(namespace, Joiner.on(EOL).join(directives), registry, context);
  }

  public GrammarBasedParser(String namespace, String recipe, DirectiveRegistry registry, DirectiveContext context) {
    this.namespace = namespace;
    this.recipe = recipe;
    this.registry = registry;
    this.context = context;
  }

  /**
   * Parses the recipe provided to this class and instantiate a list of {@link Directive} from the recipe.
   *
   * @return List of {@link Directive}.
   */
  @Override
  public List<Directive> parse() throws RecipeException {
    AtomicInteger directiveIndex = new AtomicInteger();
    try {
      List<Directive> result = new ArrayList<>();

      new GrammarWalker(new RecipeCompiler(), context).walk(recipe, (command, tokenGroup) -> {
        directiveIndex.getAndIncrement();
        DirectiveInfo info = registry.get(namespace, command);
        if (info == null) {
          throw new DirectiveNotFoundException(
            String.format("Directive '%s' not found in system and user scope. Check the name of directive.", command)
          );
        }

        try {
          Directive directive = info.instance();
          UsageDefinition definition = directive.define();
          Arguments arguments = new MapArguments(definition, tokenGroup);
          directive.initialize(arguments);
          result.add(directive);

        } catch (IllegalAccessException | InstantiationException e) {
          throw new DirectiveLoadException(e.getMessage(), e);
        }
      });

      return result;
    } catch (DirectiveLoadException | DirectiveNotFoundException | DirectiveParseException e) {
      throw new RecipeException(e.getMessage(), e, directiveIndex.get());
    } catch (Exception e) {
      throw new RecipeException(e.getMessage(), e);
    }
  }
}
