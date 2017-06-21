package co.cask.wrangler.parser;

import co.cask.wrangler.api.parser.ParsedTokens;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.IOException;
import java.io.StringReader;

/**
 * Class description here.
 */
public final class RecipeCompiler {

  public ParsedTokens compile(String recipe) throws IOException {
    CharStream stream = new ANTLRInputStream(new StringReader(recipe));
    DirectivesLexer lexer = new DirectivesLexer(stream);
    DirectivesParser parser = new DirectivesParser(new CommonTokenStream(lexer));
    ParseTree tree = parser.directives();
    parser.setBuildParseTree(false);
    RecipeVisitor visitor = new RecipeVisitor();
    visitor.visit(tree);
    return visitor.getTokens();
  }
}
