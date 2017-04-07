package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.ErrorRecord;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.PipelineExecutor;
import co.cask.wrangler.internal.TextDirectives;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Tests {@link ErrorOnCondition}
 */
public class ErrorOnConditionTest {

  @Test
  public void testErrorConditionTrue() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "error-if-true C == 1",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "A,B,C,D"),
      new Record("body", "X,Y,1,2.0"),
      new Record("body", "U,V,2,3.0")
    );

    TextDirectives directives1 = new TextDirectives(directives);
    PipelineExecutor executor = new PipelineExecutor();
    executor.configure(directives1, null);
    List<Record> results = executor.execute(records);
    List<ErrorRecord> errors = executor.errors();
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("2.0", errors.get(0).getRecord().getValue("D"));
    Assert.assertEquals("2", results.get(0).getValue("C"));
  }

  @Test
  public void testErrorConditionFalse() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "error-if-false C == 1",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "A,B,C,D"),
      new Record("body", "X,Y,1,2.0"),
      new Record("body", "U,V,2,3.0")
    );

    TextDirectives directives1 = new TextDirectives(directives);
    PipelineExecutor executor = new PipelineExecutor();
    executor.configure(directives1, null);
    List<Record> results = executor.execute(records);
    List<ErrorRecord> errors = executor.errors();
    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("2.0", results.get(0).getValue("D"));
    Assert.assertEquals("2", errors.get(0).getRecord().getValue("C"));
  }

  @Test
  public void testJsonObject() throws Exception {
    Set<String> charsets = Charset.availableCharsets().keySet();
    JsonArray array = new JsonArray();
    array.add(new JsonPrimitive("a"));
    array.add(new JsonPrimitive("b"));
    JsonObject response = new JsonObject();
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "success");
    response.addProperty("count", charsets.size());
    response.add("values", array);
    System.out.println(response);
  }

}