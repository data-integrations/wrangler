/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler;

import org.mozilla.universalchardet.UniversalDetector;

import java.io.UnsupportedEncodingException;

/**
 * Class description here.
 */
public final class BytesDecoder {
  private static final String DEFAULT_ENCODING = "UTF-8";

  /**
   * This method attempts to detect the encoding type of the byte array
   * being passed to it.
   *
   * @return String representation of the encoding.
   */
  public static String guessEncoding(byte[] bytes) {
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
