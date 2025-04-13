package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

  @Test
  public void testParsing() {
    ByteSize bs = new ByteSize("10KB");
    Assert.assertEquals(10 * 1024L, bs.getBytes());

    bs = new ByteSize("1.5MB");
    Assert.assertEquals((long) (1.5 * 1024 * 1024), bs.getBytes());
  }

  @Test
  public void testGigabytes() {
    ByteSize bs = new ByteSize("2GB");
    Assert.assertEquals(2L * 1024 * 1024 * 1024, bs.getBytes());
  }

  @Test
  public void testBytes() {
    ByteSize bs = new ByteSize("512B");
    Assert.assertEquals(512L, bs.getBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    new ByteSize("10XB");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    new ByteSize("KB10");
  }
}
