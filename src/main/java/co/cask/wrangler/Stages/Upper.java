package co.cask.wrangler.Stages;

import co.cask.wrangler.WrangleStep;
import co.cask.wrangler.Row;

/**
 * Created by nitin on 12/3/16.
 */
public class Upper implements WrangleStep {
  private String col;

  public Upper(String col) {
    this.col = col;
  }

  @Override
  public Row execute(Row row) {
    return null;
  }
}
