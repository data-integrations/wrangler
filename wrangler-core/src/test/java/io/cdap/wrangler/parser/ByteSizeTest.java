package io.cdap.wrangler.parser;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TokenType; 

public class ByteSizeTest {

  @Test
  public void testValidByteSizes() {
    assertEquals(1024L, new ByteSize("1 KB").getBytes());
    assertEquals(1048576L, new ByteSize("1 MB").getBytes());
    assertEquals(1073741824L, new ByteSize("1 GB").getBytes());
    assertEquals(1L, new ByteSize("1 B").getBytes());
    assertEquals(1536L, new ByteSize("1.5 KB").getBytes());
    assertEquals(1099511627776L, new ByteSize("1 TB").getBytes());
    assertEquals(1125899906842624L, new ByteSize("1 PB").getBytes());
  }

  @Test
  public void testCaseInsensitivity() {
    assertEquals(1024L, new ByteSize("1 kb").getBytes());
    assertEquals(1048576L, new ByteSize("1 mB").getBytes());
  }

  @Test
  public void testInvalidFormat() {
    assertThrows(IllegalArgumentException.class, () -> new ByteSize("one KB"));
    assertThrows(IllegalArgumentException.class, () -> new ByteSize("100"));
    assertThrows(IllegalArgumentException.class, () -> new ByteSize("100 XB"));
  }

  @Test
  public void testWhitespaceHandling() {
    assertEquals(2048L, new ByteSize("   2   KB   ").getBytes());
  }

  @Test
  public void testToJson() {
    ByteSize size = new ByteSize("2 KB");
    assertEquals("2048", size.toJson().getAsString());
  }

  @Test
  public void testType() {
    ByteSize size = new ByteSize("1 MB");
    assertEquals(TokenType.BYTE_SIZE, size.type());
  }

  @Test
  public void testValueMethod() {
    ByteSize size = new ByteSize("3 KB");
    assertEquals(3072L, size.value());
  }
}