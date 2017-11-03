package co.cask.wrangler;

import org.mozilla.universalchardet.UniversalDetector;

import java.io.UnsupportedEncodingException;

/**
 * Class description here.
 */
public final class BytesDecoder {

  /**
   * This method attempts to detect the encoding type of the byte array
   * being passed to it.
   *
   * @return String representation of the encoding.
   */
  public static String guessEncoding(byte[] bytes) {
    String DEFAULT_ENCODING = "UTF-8";
    UniversalDetector detector = new UniversalDetector(null);
    detector.handleData(bytes, 0, bytes.length);
    detector.dataEnd();
    String encoding = detector.getDetectedCharset();
    detector.reset();
    if (encoding == null) {
      encoding = DEFAULT_ENCODING;
    }
    return encoding;
  }

  public static String[] toLines(byte[] bytes)
    throws UnsupportedEncodingException {
    String data = new String(bytes, guessEncoding(bytes));
    String[] lines = data.split("\r\n|\r|\n");
    return lines;
  }
}
