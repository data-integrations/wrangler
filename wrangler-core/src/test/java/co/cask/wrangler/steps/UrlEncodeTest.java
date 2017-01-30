package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link UrlEncode}
 */
public class UrlEncodeTest {

  @Test
  public void testUrlEncoding() throws Exception {
    String[] directives = new String[] {
      "url-encode url",
    };

    List<Record> records = Arrays.asList(
      new Record("url", "http://www.yahoo.com?a=b c&b=ab&xyz=1")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("http%3A%2F%2Fwww.yahoo.com%3Fa%3Db+c%26b%3Dab%26xyz%3D1", records.get(0).getValue("url"));
  }
}