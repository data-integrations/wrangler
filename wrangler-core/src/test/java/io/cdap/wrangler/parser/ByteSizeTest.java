
package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteSizeTest {
  @Test
  public void testByteSizeParsing() {
    assertEquals(1024, new ByteSize("1KB").getBytes());
    assertEquals(1024 * 1024, new ByteSize("1MB").getBytes());
    assertEquals(1000, new ByteSize("1KB").getBytes());
    assertEquals(1.5 * 1024 * 1024, new ByteSize("1.5MB").getBytes(), 0.001);
  }
}
