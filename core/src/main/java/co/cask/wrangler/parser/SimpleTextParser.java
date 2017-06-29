/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveContext;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import javax.annotation.Nullable;

/**
 * Parses the DSL into specification containing stepRegistry for wrangling.
 *
 * Following are some of the commands and format that {@link SimpleTextParser}
 * will handle.
 */
public class SimpleTextParser implements RecipeParser {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleTextParser.class);

  // directives for wrangling.
  private String[] directives;

  // Usage Registry
  private static final UsageRegistry usageRegistry = new UsageRegistry();

  // Specifies the context for directive parsing.
  private DirectiveContext context;

  public SimpleTextParser(String[] directives) {
    this.directives = directives;
    this.context = new NoOpDirectiveContext();
  }

  public SimpleTextParser(String directives) {
    this(directives.split("\n"));
  }

  public SimpleTextParser(List<String> directives) {
    this(directives.toArray(new String[directives.size()]));
  }

  /**
   * Parses the DSL to generate a sequence of stepRegistry to be executed by {@link RecipePipeline}.
   *
   * The transformation parsing here needs a better solution. It has many limitations and having different way would
   * allow us to provide much more advanced semantics for directives.
   *
   * @return List of stepRegistry to be executed.
   * @throws ParseException
   */
  @Override
  public List<Directive> parse() throws DirectiveParseException {
    List<Directive> directives = new ArrayList<>();

    // Split directive by EOL
    int lineno = 1;

    // Iterate through each directive and create necessary stepRegistry.
    for (String directive : this.directives) {
      directive = directive.trim();
      if (directive.isEmpty() || directive.startsWith("//") || directive.startsWith("#")) {
        continue;
      }

      StringTokenizer tokenizer = new StringTokenizer(directive, " ");
      String command = tokenizer.nextToken();

      // Check if a directive has been aliased and if it's aliased then retrieve root command it's mapped
      // to.
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
    }
    return directives;
  }

  // If there are more tokens, then it proceeds with parsing, else throws exception.
  public static String getNextToken(StringTokenizer tokenizer, String directive,
                          String field, int lineno) throws DirectiveParseException {
    return getNextToken(tokenizer, null, directive, field, lineno, false);
  }

  public static String getNextToken(StringTokenizer tokenizer, String delimiter,
                              String directive, String field, int lineno) throws DirectiveParseException {
    return getNextToken(tokenizer, delimiter, directive, field, lineno, false);
  }

  public static String getNextToken(StringTokenizer tokenizer, String delimiter,
                          String directive, String field, int lineno, boolean optional)
    throws DirectiveParseException {
    String value = null;
    if (tokenizer.hasMoreTokens()) {
      if (delimiter == null) {
        value = tokenizer.nextToken().trim();
      } else {
        value = tokenizer.nextToken(delimiter).trim();
      }
    } else {
      if (!optional) {
        String usage = usageRegistry.getUsage(directive);
        throw new DirectiveParseException(
          String.format("Missing field '%s' at line number %d for directive <%s> (usage: %s)",
                        field, lineno, directive, usage)
        );
      }
    }
    return value;
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
