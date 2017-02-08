package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link WriteToJsonMap}
 */
public class WriteToJsonMapTest {

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
  }
}
