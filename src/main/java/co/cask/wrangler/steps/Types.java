package co.cask.wrangler.steps;

import co.cask.wrangler.WrangleStep;
import co.cask.wrangler.Row;
import co.cask.wrangler.WrangleStepException;

import java.util.List;

/**
 * Created by nitin on 12/3/16.
 */
public class Types implements WrangleStep {

  public Types(List<String> types) {
    super();
  }

  @Override
  public List<Row> execute(List<Row> rows) throws WrangleStepException {
    return null;
  }
}
