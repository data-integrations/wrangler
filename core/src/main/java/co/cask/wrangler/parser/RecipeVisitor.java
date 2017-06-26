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
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TextList;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
 */
public final class RecipeVisitor extends DirectivesBaseVisitor<CompiledUnit.Builder> {
  private CompiledUnit.Builder tokens = new CompiledUnit.Builder();

  public CompiledUnit getCompiledUnit() {
    return tokens.build();
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
  public CompiledUnit.Builder visitPragmaLoadDirective(DirectivesParser.PragmaLoadDirectiveContext ctx) {
    return super.visitPragmaLoadDirective(ctx);
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
  public CompiledUnit.Builder visitPragmaVersion(DirectivesParser.PragmaVersionContext ctx) {
    return super.visitPragmaVersion(ctx);
  }

  @Override
  public CompiledUnit.Builder visitNumberRanges(DirectivesParser.NumberRangesContext ctx) {
    List<DirectivesParser.NumberRangeContext> ranges = ctx.numberRange();
    for(DirectivesParser.NumberRangeContext range : ranges) {
      List<TerminalNode> numbers = range.Number();
      System.out.println("Range 1 : " + numbers.get(0).getText());
      System.out.println("Range 2 : " + numbers.get(0).getText());
      System.out.println("Value : " + range.value().getText());
    }
    return super.visitNumberRanges(ctx);
  }


  @Override
  public CompiledUnit.Builder visitEcommand(DirectivesParser.EcommandContext ctx) {
    tokens.add(new DirectiveName(ctx.Identifier().getText()));
    return super.visitEcommand(ctx);
  }

  @Override
  public CompiledUnit.Builder visitColumn(DirectivesParser.ColumnContext ctx) {
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
  public CompiledUnit.Builder visitText(DirectivesParser.TextContext ctx) {
    String value = ctx.String().getText();
    tokens.add(new Text(value.substring(1, value.length()-1)));
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
  public CompiledUnit.Builder visitNumber(DirectivesParser.NumberContext ctx) {
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
  public CompiledUnit.Builder visitBool(DirectivesParser.BoolContext ctx) {
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
  public CompiledUnit.Builder visitCondition(DirectivesParser.ConditionContext ctx) {
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
  public CompiledUnit.Builder visitCommand(DirectivesParser.CommandContext ctx) {
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
  public CompiledUnit.Builder visitColList(DirectivesParser.ColListContext ctx) {
    List<TerminalNode> columns = ctx.Column();
    List<String> names = new ArrayList<>();
    for (TerminalNode column : columns) {
      names.add(column.getText().substring(1));
    }
    tokens.add(new ColumnNameList(names));
    return super.visitColList(ctx);
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
  public CompiledUnit.Builder visitNumberList(DirectivesParser.NumberListContext ctx) {
    List<TerminalNode> numbers = ctx.Number();
    List<LazyNumber> numerics = new ArrayList<>();
    for (TerminalNode number : numbers) {
      numerics.add(new LazyNumber(number.getText()));
    }
    tokens.add(new NumericList(numerics));
    return super.visitNumberList(ctx);
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
  public CompiledUnit.Builder visitBoolList(DirectivesParser.BoolListContext ctx) {
    List<TerminalNode> bools = ctx.Bool();
    List<Boolean> booleans = new ArrayList<>();
    for (TerminalNode bool : bools) {
      booleans.add(Boolean.parseBoolean(bool.getText()));
    }
    tokens.add(new BoolList(booleans));
    return super.visitBoolList(ctx);
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
  public CompiledUnit.Builder visitStringList(DirectivesParser.StringListContext ctx) {
    List<TerminalNode> strings = ctx.String();
    List<String> strs = new ArrayList<>();
    for (TerminalNode string : strings) {
      strs.add(string.getText());
    }
    tokens.add(new TextList(strs));
    return super.visitStringList(ctx);
  }
}
