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
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveContext;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveNotFoundException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.parser.DirectiveName;
import co.cask.wrangler.api.parser.SyntaxError;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.api.DirectiveInfo;
import co.cask.wrangler.api.DirectiveRegistry;
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
  private List<Directive> directives;
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
   * Generates a configured set of {@link Directive} to be executed.
   *
   * @return List of {@link Directive}.
   */
  @Override
  public List<Directive> parse()
    throws DirectiveLoadException, DirectiveNotFoundException, DirectiveParseException {
    try {
      CompiledUnit compiled = compiler.compile(recipe);
      if (compiler.hasErrors()) {
        Iterator<SyntaxError> errors = compiler.getSyntaxErrors();
        throw new DirectiveParseException("Error in parsing record.");
      }
      Iterator<TokenGroup> tokenGroups = compiled.iterator();
      while(tokenGroups.hasNext()) {
        TokenGroup next = tokenGroups.next();
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
        UDD directive = info.instance();
        UsageDefinition definition = directive.define();
        Arguments arguments = new MapArguments(definition, next);
        directive.initialize(arguments);
        directives.add(directive);
      }
    } catch (CompileException | IllegalAccessException | InstantiationException  e) {
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
