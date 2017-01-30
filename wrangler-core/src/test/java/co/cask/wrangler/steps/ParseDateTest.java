package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link ParseDate}
 */
public class ParseDateTest {

  @Test
  public void testBasicDateParser() throws Exception {
    String[] directives = new String[] {
      "parse-as-date date US/Eastern",
      "format-date date_1 MM/dd/yyyy HH:mm"
    };

    List<Record> records = Arrays.asList(
      new Record("date", "now"),
      new Record("date", "today"),
      new Record("date", "12/10/2016"),
      new Record("date", "12/10/2016 06:45 AM"),
      new Record("date", "september 7th 2016"),
      new Record("date", "1485800109")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 6);
  }

}