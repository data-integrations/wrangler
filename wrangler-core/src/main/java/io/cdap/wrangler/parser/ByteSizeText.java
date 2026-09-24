package io.cdap.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {
  @Test
  public void testKB() {
    ByteSize b = new ByteSize("10kb");
    Assert.assertEquals(10240L, b.getBytes());
  }

  @Test
  public void testMB() {
    ByteSize b = new ByteSize("1.5MB");
    Assert.assertEquals(1572864L, b.getBytes());
  }

  @Test
  public void testGB() {
    ByteSize b = new ByteSize("1GB");
    Assert.assertEquals(1073741824L, b.getBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalid() {
    new ByteSize("1ZB");
  }
}
