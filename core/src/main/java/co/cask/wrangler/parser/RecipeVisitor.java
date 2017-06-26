package co.cask.wrangler.parser;

import co.cask.wrangler.api.LazyNumber;
import co.cask.wrangler.api.Triplet;
import co.cask.wrangler.api.parser.Bool;
import co.cask.wrangler.api.parser.BoolList;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.ColumnNameList;
import co.cask.wrangler.api.parser.DirectiveName;
import co.cask.wrangler.api.parser.Expression;
import co.cask.wrangler.api.parser.Numeric;
import co.cask.wrangler.api.parser.NumericList;
import co.cask.wrangler.api.parser.Properties;
import co.cask.wrangler.api.parser.Ranges;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TextList;
import co.cask.wrangler.api.parser.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class description here.
 */
public final class RecipeVisitor extends DirectivesBaseVisitor<CompiledUnit.Builder> {
  private CompiledUnit.Builder builder = new CompiledUnit.Builder();

  public CompiledUnit getCompiledUnit() {
    return builder.build();
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
  public CompiledUnit.Builder visitPropertyList(DirectivesParser.PropertyListContext ctx) {
    Map<String, Token> props = new HashMap<>();
    List<DirectivesParser.PropertyContext> properties = ctx.property();
    for(DirectivesParser.PropertyContext property : properties) {
      String identifier = property.Identifier().getText();
      Token token;
      if (property.number() != null) {
        token = new Numeric(new LazyNumber(property.number().getText()));
      } else if (property.bool() != null) {
        token = new Bool(Boolean.valueOf(property.bool().getText()));
      } else {
        token = new Text(property.text().getText());
      }
      props.put(identifier, token);
    }
    builder.add(new Properties(props));
    return builder;
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
    List<TerminalNode> identifiers = ctx.identifierList().Identifier();
    for (TerminalNode identifier : identifiers) {
      builder.addLoadableDirective(identifier.getText());
    }
    return builder;
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
    builder.addVersion(ctx.Number().getText());
    return builder;
  }

  @Override
  public CompiledUnit.Builder visitNumberRanges(DirectivesParser.NumberRangesContext ctx) {
    List<Triplet<Numeric, Numeric,String>> output = new ArrayList<>();
    List<DirectivesParser.NumberRangeContext> ranges = ctx.numberRange();
    for(DirectivesParser.NumberRangeContext range : ranges) {
      List<TerminalNode> numbers = range.Number();
      Triplet<Numeric, Numeric, String> val =
        new Triplet<>(new Numeric(new LazyNumber(numbers.get(0).getText())),
                      new Numeric(new LazyNumber(numbers.get(1).getText())),
                      range.value().getText()
        );
      output.add(val);
    }
    builder.add(new Ranges(output));
    return builder;
  }


  @Override
  public CompiledUnit.Builder visitEcommand(DirectivesParser.EcommandContext ctx) {
    builder.add(new DirectiveName(ctx.Identifier().getText()));
    return builder;
  }

  @Override
  public CompiledUnit.Builder visitColumn(DirectivesParser.ColumnContext ctx) {
    builder.add(new ColumnName(ctx.Column().getText().substring(1)));
    return builder;
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
    builder.add(new Text(value.substring(1, value.length()-1)));
    return builder;
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
    builder.add(new Numeric(number));
    return builder;
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
    builder.add(new Bool(Boolean.valueOf(ctx.Bool().getText())));
    return builder;
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
    builder.add(new Expression(sb.toString()));
    return builder;
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
    builder.add(new DirectiveName(ctx.Identifier().getText()));
    return builder;
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
    builder.add(new ColumnNameList(names));
    return builder;
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
    builder.add(new NumericList(numerics));
    return builder;
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
    builder.add(new BoolList(booleans));
    return builder;
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
    builder.add(new TextList(strs));
    return builder;
  }
}
