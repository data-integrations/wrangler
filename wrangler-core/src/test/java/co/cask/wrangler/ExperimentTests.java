package co.cask.wrangler;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Class description here.
 */
public class ExperimentTests {

  private String getIdFromName(String name) {
    name = name.toLowerCase();
    name = name.replaceAll("[_ \t]+", "-");
    name = name.replaceAll("[/$%#@**&()!,~+=?><|}{]+", "");
    return name;
  }

  @Test
  public void testIdCreationFromName() throws Exception {
    String[] names = {
      "My Sample Recipe",
      "SSGT Transformation Recipe!",
      "{SSGT Transformation Recipe!}",
      "{SSGT Transformation Recipe!}<sample-file>",
      "test>???>>>>window",
      "test    test1",
      "window\t    \t   window1"
    };

    String[] expected = {
      "my-sample-recipe",
      "ssgt-transformation-recipe",
      "ssgt-transformation-recipe",
      "ssgt-transformation-recipesample-file",
      "testwindow",
      "test-test1",
      "window-window1"
    };

    for (int i = 0; i < names.length; ++i) {
      String name = names[i];
      String expect = expected[i];
      String id = getIdFromName(name);
      Assert.assertEquals(expect, id);
    }
  }

  @Test
  public void testCurrencyParsing() throws Exception {
    NumberFormat fmt = NumberFormat.getCurrencyInstance();
    ((DecimalFormat) fmt).setParseBigDecimal(true);
    BigDecimal value = (BigDecimal)fmt.parse("1.234,56");
    value.doubleValue();
    Assert.assertEquals(1234.56, value);
  }

}
