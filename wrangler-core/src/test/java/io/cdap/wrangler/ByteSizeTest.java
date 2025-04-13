package io.cdap.wrangler;

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {
  @Test
  public void testByteConversions() {
    Assert.assertEquals(10240, new ByteSize("10KB").getBytes());
    Assert.assertEquals(5242880, new ByteSize("5MB").getBytes());
    Assert.assertEquals(2684354560L, new ByteSize("2.5GB").getBytes());
  }
}