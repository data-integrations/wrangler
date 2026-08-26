package io.cdap.wrangler.directive;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.DirectiveContext;
import io.cdap.wrangler.api.parser.DirectiveParseException;
import io.cdap.wrangler.api.parser.DirectiveParser;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Directive;
import io.cdap.wrangler.api.parser.RecipeSymbol;
import io.cdap.wrangler.api.parser.RecipeSymbolVisitor;
import io.cdap.wrangler.api.parser.SyntaxError;
import io.cdap.wrangler.api.DirectiveInfo;
import io.cdap.wrangler.api.parser.TokenType;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive that parses byte size and time duration columns into their respective numeric values.
 */
@DirectiveInfo(
  name = "parse-types",
  description = "Parses a column value as byte size or time duration to numeric format"
)
public class ParseTypes implements Directive {
  private String column;
  private String type;

  @Override
  public List<RecipeSymbol> define() {
    List<RecipeSymbol> args = new ArrayList<>();
    args.add(new ColumnName("column"));  // Column to parse
    args.add(new io.cdap.wrangler.api.parser.Text("type")); // Type: "bytes" or "duration"
    return args;
  }

  @Override
  public void initialize(DirectiveContext ctx, List<Token> args) throws DirectiveParseException {
    if (args.size() != 2) {
      throw new DirectiveParseException("parse-types directive expects exactly 2 arguments: column and type.");
    }

    column = ((ColumnName) args.get(0)).value();
    type = ((io.cdap.wrangler.api.parser.Text) args.get(1)).value().toLowerCase();
  }

  @Override
  public List<Row> execute(List<Row> rows, DirectiveContext context) {
    List<Row> results = new ArrayList<>();

    for (Row row : rows) {
      Object val = row.getValue(column);
      if (val == null || !(val instanceof String)) {
        results.add(row);
        continue;
      }

      String stringVal = (String) val;
      try {
        switch (type) {
          case "bytes":
            long bytes = ByteSize.parse(stringVal).toBytes();
            row.setValue(column, bytes);
            break;
          case "duration":
            long millis = TimeDuration.parse(stringVal).toMilliseconds();
            row.setValue(column, millis);
            break;
          default:
            throw new IllegalArgumentException("Unknown type '" + type + "'. Expected 'bytes' or 'duration'.");
        }
      } catch (Exception e) {
        // You may log the error here or choose to skip/keep the original
        row.setValue(column, null);  // Optionally set to null on error
      }

      results.add(row);
    }

    return results;
  }
}