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
import io.cdap.wrangler.api.CompileException;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveNotFoundException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.Executor;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.parser.DirectiveName;
import io.cdap.wrangler.api.parser.SyntaxError;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.DirectiveRegistry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This class <code>GrammarBasedParser</code> is an implementation of <code>RecipeParser</code>.
 * It's responsible for compiling the recipe and checking all the directives exist before concluding
 * that the directives are ready for execution.
 */
public class GrammarBasedParser implements RecipeParser {
  private static final char EOL = '\n';
  private final String namespace;
  private final Compiler compiler = new RecipeCompiler();
  private final DirectiveRegistry registry;
  private final String recipe;
  private final List<Executor> directives;
  private DirectiveContext context;

  public GrammarBasedParser(String namespace, String[] directives, DirectiveRegistry registry) {
    this(namespace, Joiner.on(EOL).join(directives), registry);
  }

  public GrammarBasedParser(String namespace, String recipe, DirectiveRegistry registry) {
    this.namespace = namespace;
    this.recipe = recipe;
    this.registry = registry;
    this.directives = new ArrayList<>();
    this.context = new NoOpDirectiveContext();
  }

  /**
   * Generates a configured set of {@link Executor} to be executed.
   *
   * @return List of {@link Executor}.
   */
  @Override
  public List<Executor> parse()
    throws DirectiveLoadException, DirectiveNotFoundException, DirectiveParseException {
    try {
      CompileStatus status = compiler.compile(recipe);
      if (!status.isSuccess()) {
        Iterator<SyntaxError> errors = status.getErrors();
        throw new DirectiveParseException(errors.next().getMessage(), errors);
      }

      Iterator<TokenGroup> tokenGroups = status.getSymbols().iterator();
      while (tokenGroups.hasNext()) {
        TokenGroup next = tokenGroups.next();
        if (next == null) {
          continue;
        }
        String command = ((DirectiveName) next.get(0)).value();
        String root = command;
        if (context.hasAlias(root)) {
          root = context.getAlias(command);
        }

        // Checks if the directive has been excluded from being used.
        if (!root.equals(command) && context.isExcluded(command)) {
          throw new DirectiveParseException(
            command, String.format("Aliased directive '%s' has been configured as restricted directive and "
                                     + "is hence unavailable. Please contact your administrator", command)
          );
        }

        if (context.isExcluded(root)) {
          throw new DirectiveParseException(
            command, String.format("Directive '%s' has been configured as restricted directive and is hence " +
                                     "unavailable. Please contact your administrator", command));
        }

        DirectiveInfo info = registry.get(namespace, root);
        if (info == null) {
          throw new DirectiveNotFoundException(
            String.format("Directive '%s' not found in system and user scope. Check the name of directive.", command)
          );
        }
        Directive directive = info.instance();
        UsageDefinition definition = directive.define();
        Arguments arguments = new MapArguments(definition, next);
        directive.initialize(arguments);
        directives.add(directive);
      }
    } catch (CompileException e) {
      throw new DirectiveParseException(e.getMessage(), e.iterator());
    } catch (IllegalAccessException | InstantiationException  e) {
      throw new DirectiveParseException(e.getMessage());
    }
    return directives;
  }

  /**
   * Initialises the directive with a {@link DirectiveContext}.
   *
   * @param context
   */
  @Nullable
  @Override
  public void initialize(DirectiveContext context) {
    if (context == null) {
      this.context = new NoOpDirectiveContext();
    } else {
      this.context = context;
    }
  }
}
