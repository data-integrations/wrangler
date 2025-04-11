/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.parser.Bool;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenGroup;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.grammar.DirectivesParser;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.DirectiveRegistry;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Grammar based parser for parsing directives.
 */
public class GrammarBasedParser extends DirectivesBaseVisitor<Token> {
  private final DirectiveRegistry registry;
  private final TokenGroup tokenGroup;
  private DirectiveInfo directive;

  public GrammarBasedParser(DirectiveRegistry registry) {
    this.registry = registry;
    this.tokenGroup = new TokenGroup();
  }

  @Override
  public Token visitCommand(DirectivesParser.CommandContext ctx) {
    String command = ctx.Identifier().getText();
    directive = registry.get(command);
    if (directive == null) {
      throw new DirectiveParseException(
        String.format("Directive '%s' is not registered. Check if the directive is available in system or " +
                      "the directive name is misspelled.", command));
    }
    return null;
  }

  @Override
  public Token visitIdentifier(DirectivesParser.IdentifierContext ctx) {
    tokenGroup.add(new Text(ctx.Identifier().getText()));
    return null;
  }

  @Override
  public Token visitColumn(DirectivesParser.ColumnContext ctx) {
    tokenGroup.add(new ColumnName(ctx.Column().getText()));
    return null;
  }

  @Override
  public Token visitNumber(DirectivesParser.NumberContext ctx) {
    tokenGroup.add(new Numeric(ctx.Number().getText()));
    return null;
  }

  @Override
  public Token visitBool(DirectivesParser.BoolContext ctx) {
    tokenGroup.add(new Bool(ctx.Bool().getText()));
    return null;
  }

  @Override
  public Token visitText(DirectivesParser.TextContext ctx) {
    String text = ctx.String().getText();
    text = text.substring(1, text.length() - 1);
    tokenGroup.add(new Text(text));
    return null;
  }

  // New method for handling byte size values
  @Override
  public Token visitValue(DirectivesParser.ValueContext ctx) {
    if (ctx.BYTE_SIZE() != null) {
      tokenGroup.add(new ByteSize(ctx.BYTE_SIZE().getText()));
    } else if (ctx.TIME_DURATION() != null) {
      tokenGroup.add(new TimeDuration(ctx.TIME_DURATION().getText()));
    } else {
      super.visitValue(ctx);
    }
    return null;
  }

  // New method specifically for byte size
  public Token visitBYTE_SIZE(DirectivesParser.BYTE_SIZEContext ctx) {
    tokenGroup.add(new ByteSize(ctx.getText()));
    return null;
  }

  // New method specifically for time duration
  public Token visitTIME_DURATION(DirectivesParser.TIME_DURATIONContext ctx) {
    tokenGroup.add(new TimeDuration(ctx.getText()));
    return null;
  }

  @Override
  public Token visitColList(DirectivesParser.ColListContext ctx) {
    List<Token> columns = new ArrayList<>();
    for (TerminalNode node : ctx.Column()) {
      columns.add(new ColumnName(node.getText()));
    }
    tokenGroup.add(columns);
    return null;
  }

  @Override
  public Token visitNumberList(DirectivesParser.NumberListContext ctx) {
    List<Token> numbers = new ArrayList<>();
    for (TerminalNode node : ctx.Number()) {
      numbers.add(new Numeric(node.getText()));
    }
    tokenGroup.add(numbers);
    return null;
  }

  @Override
  public Token visitBoolList(DirectivesParser.BoolListContext ctx) {
    List<Token> bools = new ArrayList<>();
    for (TerminalNode node : ctx.Bool()) {
      bools.add(new Bool(node.getText()));
    }
    tokenGroup.add(bools);
    return null;
  }

  @Override
  public Token visitStringList(DirectivesParser.StringListContext ctx) {
    List<Token> strings = new ArrayList<>();
    for (TerminalNode node : ctx.String()) {
      String text = node.getText();
      text = text.substring(1, text.length() - 1);
      strings.add(new Text(text));
    }
    tokenGroup.add(strings);
    return null;
  }

  @Override
  public Token visitProperties(DirectivesParser.PropertiesContext ctx) {
    if (ctx.getChildCount() > 0) {
      ParseTree tree = ctx.getChild(0);
      if (tree instanceof ParserRuleContext) {
        ParserRuleContext context = (ParserRuleContext) tree;
        if (context.exception != null) {
          throw new DirectiveParseException(context.exception);
        }
      }
    }
    return null;
  }

  public DirectiveInfo getDirective() {
    return directive;
  }

  public TokenGroup getTokens() {
    return tokenGroup;
  }
}