package io.cdap.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

import io.cdap.wrangler.api.parser.ByteSize;

public class ByteSizeTest {

  @Test
  public void testByteSizeParsing() {
    Assert.assertEquals(10240L, new ByteSize("10KB").getBytes());
    Assert.assertEquals(1572864L, new ByteSize("1.5MB").getBytes());
    Assert.assertEquals(1073741824L, new ByteSize("1GB").getBytes());
    Assert.assertEquals(1099511627776L, new ByteSize("1TB").getBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidByteSize() {
    new ByteSize("42giga");
  }
}
