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

import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.CompileException;
import co.cask.wrangler.api.CompileStatus;
import co.cask.wrangler.api.Compiler;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveContext;
import co.cask.wrangler.api.DirectiveInfo;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveNotFoundException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.DirectiveRegistry;
import co.cask.wrangler.api.Executor;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.TokenGroup;
import co.cask.wrangler.api.parser.DirectiveName;
import co.cask.wrangler.api.parser.SyntaxError;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Class description here.
 */
public class GrammarBasedParser implements RecipeParser {
  private static final char EOL = '\n';
  private Compiler compiler = new RecipeCompiler();
  private DirectiveRegistry  registry;
  private String recipe;
  private List<Executor> directives;
  private DirectiveContext context;

  public GrammarBasedParser(String[] directives, DirectiveRegistry registry) {
    this(Joiner.on(EOL).join(directives), registry);
  }

  public GrammarBasedParser(List<String> directives, DirectiveRegistry registry) {
    this(Joiner.on(EOL).join(directives), registry);
  }

  public GrammarBasedParser(String recipe, DirectiveRegistry registry) {
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
            String.format("Aliased directive '%s' has been configured as restricted directive and is hence unavailable. " +
                            "Please contact your administrator", command)
          );
        }

        if (context.isExcluded(root)) {
          throw new DirectiveParseException(
            String.format("Directive '%s' has been configured as restricted directive and is hence unavailable. " +
                            "Please contact your administrator", command)
          );
        }

        DirectiveInfo info = registry.get(root);
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
