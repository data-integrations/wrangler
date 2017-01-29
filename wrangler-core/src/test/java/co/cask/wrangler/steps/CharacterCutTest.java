package co.cask.wrangler.steps;

import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link CharacterCut}
 */
public class CharacterCutTest {

  @Test
  public void testBasicCharacterCut() throws Exception {
    String[] directives = new String[] {
      "cut body one -c 1-3",
      "cut body two -c 5-7",
      "cut body three -c 9-13",
      "cut body four -c 15-",
      "cut body five -c 1,2,3",
      "cut body six -c -3",
      "cut body seven -c 1,2,3-5",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "one two three four five six seven eight")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals(8, records.get(0).length());
    Assert.assertEquals("one", records.get(0).getValue("one"));
    Assert.assertEquals("two", records.get(0).getValue("two"));
    Assert.assertEquals("three", records.get(0).getValue("three"));
    Assert.assertEquals("four", records.get(0).getValue("four five six seven eight"));
    Assert.assertEquals("five", records.get(0).getValue("one"));
    Assert.assertEquals("six", records.get(0).getValue("one"));
    Assert.assertEquals("seven", records.get(0).getValue("one t"));
  }

  @Test(expected = DirectiveParseException.class)
  public void testCharacterParseException() throws Exception {
    String[] directives = new String[] {
      "cut body one c 1-3", // it should be -c
    };

    List<Record> records = Arrays.asList();
    PipelineTest.execute(directives, records);
  }

}