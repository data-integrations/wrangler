package co.cask.wrangler.service.filesystem;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

/**
 * Tests {@link BoundedLineInputStream}
 */
public class BoundedLineInputStreamTest {

  @Test
  public void testBasicLineReading() throws Exception {
    InputStream stream = Explorer.class.getClassLoader().getResourceAsStream("file.extensions");
    BoundedLineInputStream blis = BoundedLineInputStream.iterator(stream, "utf-8", 10);
    int i = 0;
    try {
      while(blis.hasNext()) {
        String line = blis.next();
        i++;
      }
    } finally {
      blis.close();
    }
    Assert.assertTrue(i == 10);
  }
}