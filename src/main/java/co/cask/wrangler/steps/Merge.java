package co.cask.wrangler.steps;

import co.cask.wrangler.WrangleStep;
import co.cask.wrangler.Row;
import co.cask.wrangler.WrangleStepException;

import java.util.List;

/**
 * Created by nitin on 12/3/16.
 */
public class Merge implements WrangleStep {
  private String col1;
  private String col2;
  private String dest;
  private String delimiter;

  public Merge(String col1, String col2, String dest, String delimiter) {
    this.col1 = col1;
    this.col2 = col2;
    this.dest = dest;
    this.delimiter = delimiter;
  }


  @Override
  public List<Row> execute(List<Row> rows) throws WrangleStepException {
    return null;
  }
}
