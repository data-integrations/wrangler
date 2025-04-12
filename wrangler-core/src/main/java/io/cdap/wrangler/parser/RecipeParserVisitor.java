package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.parser.grammar.DirectivesParser;
import io.cdap.wrangler.parser.grammar.DirectivesBaseVisitor;

public class RecipeParserVisitor extends DirectivesBaseVisitor<Token> {

  @Override
  public Token visitByteSize(DirectivesParser.ByteSizeContext ctx) {
    return new ByteSize(ctx.getText());
  }

  @Override
  public Token visitTimeDuration(DirectivesParser.TimeDurationContext ctx) {
    return new TimeDuration(ctx.getText());
  }
} 