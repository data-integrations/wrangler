/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
// License and package
package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.LazyNumber;
import io.cdap.wrangler.api.RecipeSymbol;
import io.cdap.wrangler.api.SourceInfo;
import io.cdap.wrangler.api.Triplet;
import io.cdap.wrangler.api.parser.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;

public final class RecipeVisitor extends DirectivesBaseVisitor<RecipeSymbol.Builder> {
    private final RecipeSymbol.Builder builder = new RecipeSymbol.Builder();

    public RecipeSymbol getCompiledUnit() {
        return builder.build();
    }

    @Override
    public RecipeSymbol.Builder visitDirective(DirectivesParser.DirectiveContext ctx) {
        builder.createTokenGroup(getOriginalSource(ctx));
        return super.visitDirective(ctx);
    }

    @Override
    public RecipeSymbol.Builder visitIdentifier(DirectivesParser.IdentifierContext ctx) {
        builder.addToken(new Identifier(ctx.Identifier().getText()));
        return super.visitIdentifier(ctx);
    }

    @Override
    public RecipeSymbol.Builder visitPropertyList(DirectivesParser.PropertyListContext ctx) {
        Map<String, Token> props = new HashMap<>();
        for (DirectivesParser.PropertyContext property : ctx.property()) {
            String identifier = property.Identifier().getText();
            Token token;
            if (property.number() != null) {
                token = new Numeric(new LazyNumber(property.number().getText()));
            } else if (property.bool() != null) {
                token = new Bool(Boolean.parseBoolean(property.bool().getText()));
            } else {
                String text = property.text().getText();
                token = new Text(text.substring(1, text.length() - 1));
            }
            props.put(identifier, token);
        }
        builder.addToken(new Properties(props));
        return builder;
    }

    @Override
    public RecipeSymbol.Builder visitPragmaLoadDirective(DirectivesParser.PragmaLoadDirectiveContext ctx) {
        for (TerminalNode identifier : ctx.identifierList().Identifier()) {
            builder.addLoadableDirective(identifier.getText());
        }
        return builder;
    }

    @Override
    public RecipeSymbol.Builder visitPragmaVersion(DirectivesParser.PragmaVersionContext ctx) {
        builder.addVersion(ctx.Number().getText());
        return builder;
    }

    @Override
    public RecipeSymbol.Builder visitNumberRanges(DirectivesParser.NumberRangesContext ctx) {
        List<Triplet<Numeric, Numeric, String>> output = new ArrayList<>();
        for (DirectivesParser.NumberRangeContext range : ctx.numberRange()) {
            String text = range.value().getText();
            if (text.startsWith("'") && text.endsWith("'")) {
                text = text.substring(1, text.length() - 1);
            }
            output.add(new Triplet<>(
                    new Numeric(new LazyNumber(range.Number(0).getText())),
                    new Numeric(new LazyNumber(range.Number(1).getText())),
                    text
            ));
        }
        builder.addToken(new Ranges(output));
        return builder;
    }

    @Override
    public RecipeSymbol.Builder visitByteSizeRule(DirectivesParser.ByteSizeRuleContext ctx) {
        builder.addToken(new ByteSize(ctx.getText())); // Ensure the class ByteSize is implemented properly
        return builder;
    }

    @Override
    public RecipeSymbol.Builder visitTimeDurationRule(DirectivesParser.TimeDurationRuleContext ctx) {
        builder.addToken(new TimeDuration(ctx.getText())); // Ensure the class TimeDuration is implemented properly
        return builder;
    }

    private SourceInfo getOriginalSource(ParserRuleContext ctx) {
        Interval interval = new Interval(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex());
        String text = ctx.start.getInputStream().getText(interval);
        return new SourceInfo(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine(), text);
    }
    
    // ByteSize Class Implementation (Placeholder)
    public static class ByteSize implements Token {
        private final String value;
        
        public ByteSize(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    // TimeDuration Class Implementation (Placeholder)
    public static class TimeDuration implements Token {
        private final String value;
        
        public TimeDuration(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }
}

