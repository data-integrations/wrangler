package co.cask.wrangler;

import co.cask.wrangler.steps.CsvParser;
import co.cask.wrangler.steps.Lower;
import co.cask.wrangler.steps.Name;
import co.cask.wrangler.steps.Types;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by nitin on 12/3/16.
 */
public class TableTest {

  @Test
  public void foo() throws Exception {
    // During intialize
    List<WrangleStep> stages = new ArrayList<>();
    stages.add(new CsvParser(',', "col", null));
    stages.add(new Name(new ArrayList<String>()));
    stages.add(new Types(new ArrayList<String>()));
    stages.add(new Lower("col1"));


    // During transform.
    Row row = new Row();
    for (WrangleStep stage : stages) {
      row = stage.execute(row);
    }

    // Convert to structured Record and emit.


  }
}