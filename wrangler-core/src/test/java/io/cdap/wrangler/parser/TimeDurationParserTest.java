package io.cdap.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationParserTest {

  private final TimeDurationParser parser = new TimeDurationParser();

  @Test
  public void testMilliseconds() {
    Assert.assertEquals(500L, parser.parse("500ms"));
  }

  @Test
  public void testSeconds() {
    Assert.assertEquals(3000L, parser.parse("3s"));
  }

  @Test
  public void testMinutes() {
    Assert.assertEquals(60000L, parser.parse("1m"));
  }

  @Test
  public void testHours() {
    Assert.assertEquals(7200000L, parser.parse("2h"));
  }

  @Test
  public void testDays() {
    Assert.assertEquals(172800000L, parser.parse("2d"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    parser.parse("10xyz");
  }
}
