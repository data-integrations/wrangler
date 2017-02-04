package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link WriteToJson}
 */
public class WriteToJsonTest {

  @Test
  public void testWriteToJson() throws Exception {
    String[] directives = new String[] {
      "write-to-json test",
    };

    JSONObject o = new JSONObject();
    o.put("a", 1);
    o.put("b", "2");
    List<Record> records = Arrays.asList(
      new Record("url", "http://www.yahoo.com?a=b c&b=ab&xyz=1")
      .add("o", o)
      .add("i1", new Integer(1))
      .add("i2", new Double(1.8f))
    );
    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("[{\"key\":\"url\",\"value\":\"http://www.yahoo.com?a\\u003db c\\u0026b\\u003dab\\" +
                          "u0026xyz\\u003d1\"},{\"key\":\"o\",\"value\":{\"map\":{\"b\":\"2\",\"a\":1}}}," +
                          "{\"key\":\"i1\",\"value\":1},{\"key\":\"i2\",\"value\":1.7999999523162842}]",
                        records.get(0).getValue("test"));
  }



}