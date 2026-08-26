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

package io.cdap.wrangler.parser;

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
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class <code>RecipeVisitor</code> implements the visitor pattern
 * used during traversal of the AST tree. The <code>ParserTree#Walker</code>
 * invokes appropriate methods as call backs with information about the node.
 *
 * <p>In order to understand what's being invoked, please look at the grammar file
 * <tt>Directive.g4</tt></p>.
 *
 * <p>This class exposes a <code>getTokenGroups</code> method for retrieving the
 * <code>RecipeSymbol</code> after visiting. The <code>RecipeSymbol</code> represents
 * all the <code>TokenGroup</code> for all directives in a recipe. Each directive
 * will create a <code>TokenGroup</code></p>
 *
 * <p> As the <code>ParseTree</code> is walking through the call graph, it generates
 * one <code>TokenGroup</code> for each directive in the recipe. Each <code>TokenGroup</code>
 * contains parsed <code>Tokens</code> for that directive along with more information like
 * <code>SourceInfo</code>. A collection of <code>TokenGroup</code> consistutes a <code>RecipeSymbol</code>
 * that is returned by this function.</p>
 */
public final class RecipeVisitor extends DirectivesBaseVisitor<RecipeSymbol.Builder> {
  private RecipeSymbol.Builder builder = new RecipeSymbol.Builder();

  /**
   * Returns a <code>RecipeSymbol</code> for the recipe being parsed. This
   * object has all the tokens that were successfully parsed along with source
   * information for each directive in the recipe.
   *
   * @return An compiled object after parsing the recipe.
   */
  public RecipeSymbol getCompiledUnit() {
    return builder.build();
  }

  /**
   * A Recipe is made up of Directives and Directives is made up of each individual
   * Directive. This method is invoked on every visit to a new directive in the recipe.
   */
  @Override
  public RecipeSymbol.Builder visitDirective(DirectivesParser.DirectiveContext ctx) {
    builder.createTokenGroup(getOriginalSource(ctx));
    return super.visitDirective(ctx);
  }

  /**
   * A Directive can include identifiers, this method extracts that token that is being
   * identified as token of type <code>Identifier</code>.
   */
  @Override
  public RecipeSymbol.Builder visitIdentifier(DirectivesParser.IdentifierContext ctx) {
    builder.addToken(new Identifier(ctx.Identifier().getText()));
    return super.visitIdentifier(ctx);
  }

  /**
   * A Directive can include properties (which are a collection of key and value pairs),
   * this method extracts that token that is being identified as token of type <code>Properties</code>.
   */
  @Override
  public RecipeSymbol.Builder visitPropertyList(DirectivesParser.PropertyListContext ctx) {
    Map<String, Token> props = new HashMap<>();
    List<DirectivesParser.PropertyContext> properties = ctx.property();
    for (DirectivesParser.PropertyContext property : properties) {
      String identifier = property.Identifier().getText();
      Token token;
      if (property.number() != null) {
        token = new Numeric(new LazyNumber(property.number().getText()));
      } else if (property.bool() != null) {
        token = new Bool(Boolean.valueOf(property.bool().getText()));
      } else {
        String text = property.text().getText();
        token = new Text(text.substring(1, text.length() - 1));
      }
      props.put(identifier, token);
    }
    builder.addToken(new Properties(props));
    return builder;
  }

  /**
   * A Pragma is an instruction to the compiler to dynamically load the directives being specified
   * from the <code>DirectiveRegistry</code>. These do not affect the data flow.
   *
   * <p>E.g. <code>#pragma load-directives test1, test2, test3;</code> will collect the tokens
   * test1, test2 and test3 as dynamically loadable directives. <p>
   */
  @Override
  public RecipeSymbol.Builder visitPragmaLoadDirective(DirectivesParser.PragmaLoadDirectiveContext ctx) {
    List<TerminalNode> identifiers = ctx.identifierList().Identifier();
    for (TerminalNode identifier : identifiers) {
      builder.addLoadableDirective(identifier.getText());
    }
    return builder;
  }

  /**
   * A Pragma version is a informational directive to notify compiler about the grammar that is should
   * be using to parse the directives below.
   */
  @Override
  public RecipeSymbol.Builder visitPragmaVersion(DirectivesParser.PragmaVersionContext ctx) {
    builder.addVersion(ctx.Number().getText());
    return builder;
  }

  /**
   * A Directive can include number ranges like start:end=value[,start:end=value]*. This
   * visitor method allows you to collect all the number ranges and create a token type
   * <code>Ranges</code>.
   */
  @Override
  public RecipeSymbol.Builder visitNumberRanges(DirectivesParser.NumberRangesContext ctx) {
    List<Triplet<Numeric, Numeric, String>> output = new ArrayList<>();
    List<DirectivesParser.NumberRangeContext> ranges = ctx.numberRange();
    for (DirectivesParser.NumberRangeContext range : ranges) {
      List<TerminalNode> numbers = range.Number();
      String text = range.value().getText();
      if (text.startsWith("'") && text.endsWith("'")) {
        text = text.substring(1, text.length() - 1);
      }
      Triplet<Numeric, Numeric, String> val =
        new Triplet<>(new Numeric(new LazyNumber(numbers.get(0).getText())),
                      new Numeric(new LazyNumber(numbers.get(1).getText())),
                      text
        );
      output.add(val);
    }
    builder.addToken(new Ranges(output));
    return builder;
  }

  /**
   * This visitor method extracts the custom directive name specified. The custom
   * directives are specified with a bang (!) at the start.
   */
  @Override
  public RecipeSymbol.Builder visitEcommand(DirectivesParser.EcommandContext ctx) {
    builder.addToken(new DirectiveName(ctx.Identifier().getText()));
    return builder;
  }

  /**
   * A Directive can consist of column specifiers. These are columns that the directive
   * would operate on. When a token of type column is visited, it would generate a token
   * type of type <code>ColumnName</code>.
   */
  @Override
  public RecipeSymbol.Builder visitColumn(DirectivesParser.ColumnContext ctx) {
    builder.addToken(new ColumnName(ctx.Column().getText().substring(1)));
    return builder;
  }

  /**
   * A Directive can consist of text field. These type of fields are enclosed within
   * a single-quote or a double-quote. This visitor method extracts the string value
   * and creates a token of type <code>Text</code>.
   */
  @Override
  public RecipeSymbol.Builder visitText(DirectivesParser.TextContext ctx) {
    String text = ctx.String().getText();
    builder.addToken(new Text(text.substring(1, text.length() - 1)));
    return builder;
  }

  /**
   * A Directive can consist of number. This visitor method extracts the number as
   * <code>Numeric</code> token.
   */
  @Override
  public RecipeSymbol.Builder visitNumber(DirectivesParser.NumberContext ctx) {
    builder.addToken(new Numeric(new LazyNumber(ctx.Number().getText())));
    return builder;
  }

  /**
   * A Directive can consist of boolean value 'true' and 'false'. This visitor method
   * extracts the text as parsed by the <code>lexer</code> and wraps it around <code>Bool</code>
   * token type.
   */
  @Override
  public RecipeSymbol.Builder visitBool(DirectivesParser.BoolContext ctx) {
    builder.addToken(new Bool(Boolean.valueOf(ctx.Bool().getText())));
    return builder;
  }

  /**
   * A Directive can consist of a byte size value like "10KB" or "1.5MB". This visitor method
   * extracts the text as parsed by the <code>lexer</code> and wraps it around <code>ByteSize</code>
   * token type.
   */
  @Override
  public RecipeSymbol.Builder visitByteSize(DirectivesParser.ByteSizeContext ctx) {
    builder.addToken(new ByteSize(ctx.ByteSize().getText()));
    return builder;
  }

  /**
   * A Directive can consist of a time duration value like "10ms" or "1.5s". This visitor method
   * extracts the text as parsed by the <code>lexer</code> and wraps it around <code>TimeDuration</code>
   * token type.
   */
  @Override
  public RecipeSymbol.Builder visitTimeDuration(DirectivesParser.TimeDurationContext ctx) {
    builder.addToken(new TimeDuration(ctx.TimeDuration().getText()));
    return builder;
  }

  /**
   * A Directive can consist of condition or an expression. This visitor method extracts
   * the condition or expression within curly braces and creates a token of type <code>Expression</code>.
   */
  @Override
  public RecipeSymbol.Builder visitCondition(DirectivesParser.ConditionContext ctx) {
    int a = ctx.start.getStartIndex();
    int b = ctx.stop.getStopIndex();
    Interval interval = new Interval(a, b);
    String expression = ctx.start.getInputStream().getText(interval);
    builder.addToken(new Expression(expression));
    return builder;
  }

  /**
   * This visitor method extracts the directive name that is being specified
   * for operation on data.
   */
  @Override
  public RecipeSymbol.Builder visitCommand(DirectivesParser.CommandContext ctx) {
    builder.addToken(new DirectiveName(ctx.Identifier().getText()));
    return builder;
  }

  /**
   * This visitor method extractrs the collection of <code>ColumnName</code>s and
   * wraps it in a token of type <code>ColumnNameList</code>.
   */
  @Override
  public RecipeSymbol.Builder visitColList(DirectivesParser.ColListContext ctx) {
    List<String> columnNames = new ArrayList<>();
    for (TerminalNode node : ctx.Column()) {
      columnNames.add(node.getText().substring(1));
    }
    builder.addToken(new ColumnNameList(columnNames));
    return builder;
  }

  /**
   * This visitor method extracts the collection of <code>Numeric</code>s and
   * wraps it in a token of type <code>NumericList</code>.
   */
  @Override
  public RecipeSymbol.Builder visitNumberList(DirectivesParser.NumberListContext ctx) {
    List<LazyNumber> lazyNumbers = new ArrayList<>();
    for (TerminalNode node : ctx.Number()) {
      lazyNumbers.add(new LazyNumber(node.getText()));
    }
    builder.addToken(new NumericList(lazyNumbers));
    return builder;
  }

  /**
   * This visitor method extracts the collection of <code>Bool</code>s and
   * wraps it in a token of type <code>BoolList</code>.
   */
  @Override
  public RecipeSymbol.Builder visitBoolList(DirectivesParser.BoolListContext ctx) {
    List<Boolean> booleans = new ArrayList<>();
    for (TerminalNode node : ctx.Bool()) {
      booleans.add(Boolean.valueOf(node.getText()));
    }
    builder.addToken(new BoolList(booleans));
    return builder;
  }

  /**
   * This visitor method extracts the collection of <code>Text</code>s and
   * wraps it in a token of type <code>TextList</code>.
   */
  @Override
  public RecipeSymbol.Builder visitStringList(DirectivesParser.StringListContext ctx) {
    List<String> textStrings = new ArrayList<>();
    for (TerminalNode node : ctx.String()) {
      String text = node.getText();
      textStrings.add(text.substring(1, text.length() - 1));
    }
    builder.addToken(new TextList(textStrings));
    return builder;
  }

  /**
   * This method provides the source of the node being visited.
   */
  private SourceInfo getOriginalSource(ParserRuleContext ctx) {
    int a = ctx.start.getStartIndex();
    int b = ctx.stop.getStopIndex();
    Interval interval = new Interval(a, b);
    String text = ctx.start.getInputStream().getText(interval);
    return new SourceInfo(ctx.start.getLine(),
                            ctx.start.getCharPositionInLine(),
                            ctx.stop.getLine(),
                            ctx.stop.getCharPositionInLine(),
                            text);
  }
}
