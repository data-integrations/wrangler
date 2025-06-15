/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cdap.wrangler.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.cdap.wrangler.api.LazyNumber;
import io.cdap.wrangler.api.RecipeSymbol;
import io.cdap.wrangler.api.SourceInfo;
import io.cdap.wrangler.api.Triplet;
import io.cdap.wrangler.api.parser.Bool;
import io.cdap.wrangler.api.parser.BoolList;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.ColumnNameList;
import io.cdap.wrangler.api.parser.DirectiveName;
import io.cdap.wrangler.api.parser.Expression;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.NumericList;
import io.cdap.wrangler.api.parser.Properties;
import io.cdap.wrangler.api.parser.Ranges;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TextList;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;


/**
 * Visitor class to parse directives and produce RecipeSymbol.
 */
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
      String key = property.Identifier().getText();
      Token valueToken;
      if (property.number() != null) {
        valueToken = new Numeric(new LazyNumber(property.number().getText()));
      } else if (property.bool() != null) {
        valueToken = new Bool(Boolean.parseBoolean(property.bool().getText()));
      } else {
        String rawText = property.text().getText();
        valueToken = new Text(rawText.substring(1, rawText.length() - 1));
      }
      props.put(key, valueToken);
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
    List<Triplet<Numeric, Numeric, String>> ranges = new ArrayList<>();
    for (DirectivesParser.NumberRangeContext range : ctx.numberRange()) {
      String text = range.value().getText();
      if (text.startsWith("'") && text.endsWith("'")) {
        text = text.substring(1, text.length() - 1);
      }
      ranges.add(new Triplet<>(
          new Numeric(new LazyNumber(range.Number(0).getText())),
          new Numeric(new LazyNumber(range.Number(1).getText())),
          text));
    }
    builder.addToken(new Ranges(ranges));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitEcommand(DirectivesParser.EcommandContext ctx) {
    builder.addToken(new DirectiveName(ctx.Identifier().getText()));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitColumn(DirectivesParser.ColumnContext ctx) {
    builder.addToken(new ColumnName(ctx.Column().getText().substring(1)));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitText(DirectivesParser.TextContext ctx) {
    String value = ctx.String().getText();
    builder.addToken(new Text(value.substring(1, value.length() - 1)));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitNumber(DirectivesParser.NumberContext ctx) {
    builder.addToken(new Numeric(new LazyNumber(ctx.Number().getText())));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitBool(DirectivesParser.BoolContext ctx) {
    builder.addToken(new Bool(Boolean.parseBoolean(ctx.Bool().getText())));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitCondition(DirectivesParser.ConditionContext ctx) {
    StringBuilder expr = new StringBuilder();
    for (int i = 1; i < ctx.getChildCount() - 1; ++i) {
      expr.append(ctx.getChild(i).getText()).append(" ");
    }
    builder.addToken(new Expression(expr.toString().trim()));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitCommand(DirectivesParser.CommandContext ctx) {
    builder.addToken(new DirectiveName(ctx.Identifier().getText()));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitColList(DirectivesParser.ColListContext ctx) {
    List<String> columns = new ArrayList<>();
    for (TerminalNode column : ctx.Column()) {
      columns.add(column.getText().substring(1));
    }
    builder.addToken(new ColumnNameList(columns));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitNumberList(DirectivesParser.NumberListContext ctx) {
    List<LazyNumber> numbers = new ArrayList<>();
    for (TerminalNode number : ctx.Number()) {
      numbers.add(new LazyNumber(number.getText()));
    }
    builder.addToken(new NumericList(numbers));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitBoolList(DirectivesParser.BoolListContext ctx) {
    List<Boolean> bools = new ArrayList<>();
    for (TerminalNode bool : ctx.Bool()) {
      bools.add(Boolean.parseBoolean(bool.getText()));
    }
    builder.addToken(new BoolList(bools));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitStringList(DirectivesParser.StringListContext ctx) {
    List<String> strings = new ArrayList<>();
    for (TerminalNode string : ctx.String()) {
      String text = string.getText();
      strings.add(text.substring(1, text.length() - 1));
    }
    builder.addToken(new TextList(strings));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitByteSizeOne(DirectivesParser.ByteSizeOneContext ctx) {
    builder.addToken(new ByteSize(ctx.getText()));
    return builder;
  }

  @Override
  public RecipeSymbol.Builder visitTimeDurationOne(DirectivesParser.TimeDurationOneContext ctx) {
    builder.addToken(new TimeDuration(ctx.getText()));
    return builder;
  }

  private SourceInfo getOriginalSource(ParserRuleContext ctx) {
    Interval interval = new Interval(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex());
    String sourceText = ctx.getStart().getInputStream().getText(interval);
    return new SourceInfo(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine(), sourceText);
  }
}


