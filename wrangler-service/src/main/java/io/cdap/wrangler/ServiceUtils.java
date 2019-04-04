/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler;

import com.google.common.base.Charsets;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.util.encoders.Hex;

/**
 * This class provides utility services to the service in this package.
 */
public final class ServiceUtils {

  /**
   * Generates a MD5 hash for a given string.
   *
   * This implementation is based on Bouncycastle. So, you would need to initialize the security
   * provider to use {@link org.bouncycastle.jce.provider.BouncyCastleProvider}.
   *
   * <code>
   *   Security.addProvider(new BouncyCastleProvider());
   * </code>
   * @param value to be converted to MD5.
   * @return String representation of MD5.
   */
  public static String generateMD5(String value) {
    byte[] input = value.getBytes(Charsets.UTF_8);
    MD5Digest md5Digest = new MD5Digest();
    md5Digest.update(input, 0, input.length);
    byte[] output = new byte[md5Digest.getDigestSize()];
    md5Digest.doFinal(output, 0);
    return new String(Hex.encode(output));
  }
}
