package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.parser.DirectivesParser.ValueContext;

public class DirectiveParserVisitorImpl extends DirectivesBaseVisitor<Token> {

  @Override
  public Token visitValue(ValueContext ctx) {
    String text = ctx.getText();

    // Match BYTE_SIZE like "10KB", "1.5MB", etc.
    if (text.matches("(?i)^\\d+(\\.\\d+)?(B|KB|MB|GB|TB)$")) {
      return new ByteSize(text);
    }

    // Match TIME_DURATION like "10ms", "2s", etc.
    if (text.matches("(?i)^\\d+(\\.\\d+)?(ns|us|ms|s|m|h|d)$")) {
      return new TimeDuration(text);
    }

    // Fallback for other token types (e.g., string, int)
    return super.visitValue(ctx);
  }
}
