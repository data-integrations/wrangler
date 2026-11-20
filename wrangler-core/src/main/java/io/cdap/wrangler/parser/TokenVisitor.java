package io.cdap.wrangler.parser;
import io.cdap.wrangler.api.LazyNumber;

import io.cdap.wrangler.api.parser.*;
import org.antlr.v4.runtime.tree.ParseTree;

public class TokenVisitor extends DirectivesBaseVisitor<Token> {

  @Override
  public Token visitValue(DirectivesParser.ValueContext ctx) {
    ParseTree child = ctx.getChild(0);
    String text = child.getText();

    if (child instanceof DirectivesParser.TextContext) {
      return new Text(stripQuotes(text));
    }else if (child instanceof DirectivesParser.NumberContext) {
        return new Numeric(new LazyNumber(text));

      }
       else if (child instanceof DirectivesParser.BoolContext) {
      return new Bool(Boolean.parseBoolean(text));
    } else if (child instanceof DirectivesParser.ColumnContext) {
      return new ColumnName(text);
    } else if (text.matches("(?i)[0-9]+(\\.[0-9]+)?[kmg]?b")) {
      return new ByteSize(text);
    } else if (text.matches("(?i)[0-9]+(\\.[0-9]+)?(ms|s|sec|m|min)")) {
      return new TimeDuration(text);
    }

    throw new IllegalArgumentException("Unsupported value type: " + text);
  }

  private String stripQuotes(String str) {
    if ((str.startsWith("\"") && str.endsWith("\"")) || (str.startsWith("'") && str.endsWith("'"))) {
      return str.substring(1, str.length() - 1);
    }
    return str;
  }
}
