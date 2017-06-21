package co.cask.wrangler.parser;

import co.cask.wrangler.api.LazyNumber;
import co.cask.wrangler.api.parser.Bool;
import co.cask.wrangler.api.parser.BoolList;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.ColumnNameList;
import co.cask.wrangler.api.parser.DirectiveName;
import co.cask.wrangler.api.parser.Expression;
import co.cask.wrangler.api.parser.Numeric;
import co.cask.wrangler.api.parser.NumericList;
import co.cask.wrangler.api.parser.ParsedTokens;
import co.cask.wrangler.api.parser.TextList;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
 */
public final class RecipeVisitor extends DirectivesBaseVisitor<ParsedTokens.Builder> {
  private ParsedTokens.Builder tokens = new ParsedTokens.Builder();

  public ParsedTokens getTokens() {
    return tokens.build();
  }

  @Override
  public ParsedTokens.Builder visitNumberranges(DirectivesParser.NumberrangesContext ctx) {
    List<DirectivesParser.NumberrangeContext> ranges = ctx.numberrange();
    for(DirectivesParser.NumberrangeContext range : ranges) {
      List<TerminalNode> numbers = range.Number();
      System.out.println("Range 1 : " + numbers.get(0).getText());
      System.out.println("Range 2 : " + numbers.get(0).getText());
      System.out.println("Value : " + range.value().getText());
    }
    return super.visitNumberranges(ctx);
  }


  @Override
  public ParsedTokens.Builder visitEcommand(DirectivesParser.EcommandContext ctx) {
    System.out.println("ECommand : " + ctx.Identifier().getText());
    return super.visitEcommand(ctx);
  }

  @Override
  public ParsedTokens.Builder visitColumn(DirectivesParser.ColumnContext ctx) {
    tokens.add(new ColumnName(ctx.Column().getText().substring(1)));
    return super.visitColumn(ctx);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   *
   * @param ctx
   */
  @Override
  public ParsedTokens.Builder visitText(DirectivesParser.TextContext ctx) {
    String value = ctx.String().getText();
    tokens.add(new ColumnName(value.substring(1, value.length()-1)));
    return super.visitText(ctx);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   *
   * @param ctx
   */
  @Override
  public ParsedTokens.Builder visitNumber(DirectivesParser.NumberContext ctx) {
    LazyNumber number = new LazyNumber(ctx.Number().getText());
    tokens.add(new Numeric(number));
    return super.visitNumber(ctx);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   *
   * @param ctx
   */
  @Override
  public ParsedTokens.Builder visitBool(DirectivesParser.BoolContext ctx) {
    tokens.add(new Bool(Boolean.parseBoolean(ctx.Bool().getText())));
    return super.visitBool(ctx);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   *
   * @param ctx
   */
  @Override
  public ParsedTokens.Builder visitCondition(DirectivesParser.ConditionContext ctx) {
    int childCount = ctx.getChildCount();
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < childCount - 1; ++i) {
      ParseTree child = ctx.getChild(i);
      sb.append(child.getText()).append(" ");
    }
    tokens.add(new Expression(sb.toString()));
    return super.visitCondition(ctx);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   *
   * @param ctx
   */
  @Override
  public ParsedTokens.Builder visitCommand(DirectivesParser.CommandContext ctx) {
    tokens.add(new DirectiveName(ctx.Identifier().getText()));
    return super.visitCommand(ctx);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   *
   * @param ctx
   */
  @Override
  public ParsedTokens.Builder visitCollist(DirectivesParser.CollistContext ctx) {
    List<TerminalNode> columns = ctx.Column();
    System.out.println("Column List");
    List<String> names = new ArrayList<>();
    for (TerminalNode column : columns) {
      names.add(column.getText().substring(1));
    }
    tokens.add(new ColumnNameList(names));
    return super.visitCollist(ctx);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   *
   * @param ctx
   */
  @Override
  public ParsedTokens.Builder visitNumberlist(DirectivesParser.NumberlistContext ctx) {
    List<TerminalNode> numbers = ctx.Number();
    List<LazyNumber> numerics = new ArrayList<>();
    for (TerminalNode number : numbers) {
      numerics.add(new LazyNumber(number.getText()));
    }
    tokens.add(new NumericList(numerics));
    return super.visitNumberlist(ctx);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   *
   * @param ctx
   */
  @Override
  public ParsedTokens.Builder visitBoollist(DirectivesParser.BoollistContext ctx) {
    List<TerminalNode> bools = ctx.Bool();
    List<Boolean> booleans = new ArrayList<>();
    for (TerminalNode bool : bools) {
      booleans.add(Boolean.parseBoolean(bool.getText()));
    }
    tokens.add(new BoolList(booleans));
    return super.visitBoollist(ctx);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <p>The default implementation returns the result of calling
   * {@link #visitChildren} on {@code ctx}.</p>
   *
   * @param ctx
   */
  @Override
  public ParsedTokens.Builder visitStringlist(DirectivesParser.StringlistContext ctx) {
    List<TerminalNode> strings = ctx.String();
    List<String> strs = new ArrayList<>();
    for (TerminalNode string : strings) {
      strs.add(string.getText());
    }
    tokens.add(new TextList(strs));
    return super.visitStringlist(ctx);
  }
}
