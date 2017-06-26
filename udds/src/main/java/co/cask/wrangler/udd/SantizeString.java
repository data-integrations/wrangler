package co.cask.wrangler.udd;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.annotations.Usage;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.ColumnNameList;
import co.cask.wrangler.api.parser.Expression;
import co.cask.wrangler.api.parser.Numeric;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.api.RecipeContext;

import java.util.List;

/**
 * Class description here.
 */
@Plugin(type = UDD.Type)
@Name(SantizeString.DIRECTIVE_NAME)
@Usage("sanatize-string :col1 :col2 [exp:{ <expression> }] <number> :col[,:col]*")
@Description("Sanatizes String")
public final class SantizeString implements UDD {
  public static final String DIRECTIVE_NAME = "sanatize-string";
  private ColumnName col1;
  private ColumnName col2;
  private Expression expr;
  private Numeric number;
  private ColumnNameList columns;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = new UsageDefinition.Builder(SantizeString.DIRECTIVE_NAME);
    builder.define("col1", TokenType.COLUMN_NAME);
    builder.define("col2", TokenType.COLUMN_NAME);
    builder.define("expr", TokenType.EXPRESSION, Optional.TRUE);
    builder.define("number", TokenType.NUMERIC);
    builder.define("columns", TokenType.COLUMN_NAME_LIST);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    col1 = args.value("col1");
    col2 = args.value("col2");
    if (args.contains("expr")) {
      expr = args.value("expr");
    }
    number = args.value("number");
    columns = args.value("columns");
  }

  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context)
    throws DirectiveExecutionException, ErrorRecordException {
    return null;
  }

}
