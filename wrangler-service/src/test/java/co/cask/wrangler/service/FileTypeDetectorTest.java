package co.cask.wrangler.service;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link FileTypeDetector}
 */
public class FileTypeDetectorTest {

  @Test
  public void testFileTypeExtensions() throws Exception {
    FileTypeDetector detector = new FileTypeDetector();

    String[] filenames = {
      "syslog.dat",
      "syslog.dat.1",
      "syslog.txt",
      "syslog.txt.1",
      "titanic.csv",
      "titanic.csv.1",
      "titanic.csv.1.2"
    };

    for (String filename : filenames) {
      String mimeType = detector.detectFileType(filename);
      Assert.assertNotEquals("UNKNOWN", mimeType);
      Assert.assertEquals("text/plain", mimeType);
    }
  }
}