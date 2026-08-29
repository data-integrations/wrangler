package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.grammar.DirectivesBaseVisitor;
import io.cdap.wrangler.grammar.DirectivesParser;

public class CustomDirectiveVisitor extends DirectivesBaseVisitor<Token> {

    @Override
    public Token visitByteSizeArg(DirectivesParser.ByteSizeArgContext ctx) {
        return new ByteSize(ctx.getText());
    }

    @Override
    public Token visitTimeDurationArg(DirectivesParser.TimeDurationArgContext ctx) {
        return new TimeDuration(ctx.getText());
    }

    @Override
    public Token visitValue(DirectivesParser.ValueContext ctx) {
        if (ctx.BYTE_SIZE() != null) return new ByteSize(ctx.getText());
        if (ctx.TIME_DURATION() != null) return new TimeDuration(ctx.getText());
        // return other existing tokens (string, boolean, number)
        return super.visitValue(ctx);
    }
}
