package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.ErrorRecord;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.executor.TextDirectives;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SendToError}
 */
public class SendToErrorTest {

  @Test
  public void testErrorConditionTrue() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "send-to-error C == 1",
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
}
