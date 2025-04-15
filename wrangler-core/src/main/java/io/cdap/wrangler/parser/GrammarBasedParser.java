package io.cdap.wrangler.parser;

import com.google.common.base.Joiner;
import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.Token;
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

  public void initialize() {
    // No-op, retained for compatibility
  }

  /**
   * Parses a single directive line and returns its tokens.
   *
   * @param s A string directive to parse
   * @return List of parsed tokens
   */
  public List<Token> parse(String s) throws CompileException {
    RecipeCompiler compiler = new RecipeCompiler();
    List<TokenGroup> groups = compiler.compile(s).getTokens();

    List<Token> tokens = new ArrayList<>();
    for (TokenGroup group : groups) {
      tokens.addAll(group.getTokens());
    }

    return tokens;
  }
}
