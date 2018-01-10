package co.cask.directives.date;

import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link DiffDate}
 */
public class DiffDateTest {

  @Test
  public void testBasicDateDiffFunctionality() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("dt1", "12/10/2016 07:45").add("dt2", "12/10/2018 08:45"),
      new Row("dt1", "12/10/2018 07:45").add("dt2", "12/10/2016 08:45"),
      new Row("dt1", "12/10/2016 07:45").add("dt2", "12/10/2016 07:45"),
      new Row("dt1", "12/10/2016 07:45").add("dt2", "12/10/2016 07:46"),
      new Row("dt1", "12/10/2016 07:45").add("dt2", "12/10/2016 08:45"),
      new Row("dt1", "12/10/2016 07:45").add("dt2", "12/11/2016 07:45")
    );

    String[] directives = new String[] {
      "parse-as-simple-date dt1 MM/dd/yyyy HH:mm",
      "parse-as-simple-date dt2 MM/dd/yyyy HH:mm",
      "diff-date dt1 dt2 days days",
      "diff-date dt1 dt2 hours hours",
      "diff-date dt1 dt2 minutes minutes",
      "diff-date dt1 dt2 seconds seconds",
      "diff-date dt1 dt2 milliseconds milliseconds"
    };

    Pair<List<Row>, List<Row>> result = TestingRig.executeWithErrors(directives, rows);
    List<Row> results = result.getFirst();
    List<Row> errors = result.getSecond();
    Assert.assertEquals(0, errors.size());
    Assert.assertEquals(6, results.size());
  }

}