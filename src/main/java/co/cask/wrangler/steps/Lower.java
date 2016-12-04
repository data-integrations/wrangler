package co.cask.wrangler.steps;

import co.cask.wrangler.ColumnType;
import co.cask.wrangler.Row;
import co.cask.wrangler.WrangleStep;
import co.cask.wrangler.WrangleStepException;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangler step for lower casing the 'col' value of type String.
 */
public class Lower implements WrangleStep {
  private String col;

  public Lower(String col) {
    this.col = col;
  }

  @Override
  public List<Row> execute(List<Row> rows) throws WrangleStepException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      ColumnType type = row.getType(col);
      if (type == ColumnType.STRING) {
        String value = (String)row.get(col);
        value.toLowerCase();
        row.setValue(col, value);
      }
      results.add(row);
    }
    return results;
  }
}
