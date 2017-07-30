package co.cask.wrangler.service.filesystem;

import co.cask.wrangler.service.explorer.BoundedLineInputStream;
import co.cask.wrangler.service.explorer.Explorer;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link BoundedLineInputStream}
 */
public class BoundedLineInputStreamTest {

  @Test
  public void testBasicLineReading() throws Exception {
    InputStream stream = Explorer.class.getClassLoader().getResourceAsStream("file.extensions");
    BoundedLineInputStream blis = BoundedLineInputStream.iterator(stream, "utf-8", 10);
    int i = 0;
    List<String> lines = new ArrayList<>();
    try {
      while(blis.hasNext()) {
        String line = blis.next();
        lines.add(line);
        Assert.assertNotNull(line);
        i++;
      }
    } finally {
      blis.close();
    }
    Assert.assertTrue(i == 10);
  }
}