package co.cask.directives.row;

import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.ErrorRecord;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SendToErrorAndContinue}
 */
public class SendToErrorAndContinueTest {

  @Test
  public void testErrorConditionTrueAndContinue() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "send-to-error-and-continue exp:{C == 1}",
      "send-to-error-and-continue exp:{C == 2}",
      "send-to-error-and-continue exp:{D == 3.0}",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "A,B,C,D"),
      new Row("body", "X,Y,1,2.0"),
      new Row("body", "U,V,2,3.0")
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> results = pipeline.execute(rows);
    List<ErrorRecord> errors = pipeline.errors();

    Assert.assertEquals(3, errors.size());
    Assert.assertEquals(2, results.size());
  }
}