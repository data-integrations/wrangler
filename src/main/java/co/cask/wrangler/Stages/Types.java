package co.cask.wrangler.Stages;

import co.cask.wrangler.WrangleStep;
import co.cask.wrangler.Row;

import java.util.List;

/**
 * Created by nitin on 12/3/16.
 */
public class Types implements WrangleStep {

  public Types(List<String> types) {
    super();
  }

  @Override
  public Row execute(Row row) {
    return null;
  }
}
