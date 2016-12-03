package co.cask.wrangler.Stages;

import co.cask.wrangler.WrangleStep;
import co.cask.wrangler.Row;
import co.cask.wrangler.WrangleStepException;

import java.util.List;

/**
 * Created by nitin on 12/3/16.
 */
public class Name implements WrangleStep {
  public Name(List<String> stringArrayList) {
    super();
  }

  @Override
  public Row execute(Row row) throws WrangleStepException {
    return null;
  }
}
