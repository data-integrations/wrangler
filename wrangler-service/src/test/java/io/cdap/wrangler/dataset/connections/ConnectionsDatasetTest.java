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

package io.cdap.wrangler.dataset.connections;

import org.junit.Assert;
import org.junit.Test;

import java.security.Key;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * Class description here.
 */
public class ConnectionsDatasetTest {

  @Test
  public void testEncryptionAndDecryption() throws Exception {
    String text = "Hello World";
    String key = "Bar12345Bar12345"; // 128 bit key
    // Create key and cipher
    Key aesKey = new SecretKeySpec(key.getBytes(), "AES");
    Cipher cipher = Cipher.getInstance("AES");
    // encrypt the text
    cipher.init(Cipher.ENCRYPT_MODE, aesKey);
    byte[] encrypted = cipher.doFinal(text.getBytes());
    // decrypt the text
    cipher.init(Cipher.DECRYPT_MODE, aesKey);
    String decrypted = new String(cipher.doFinal(encrypted));
    Assert.assertEquals(text, decrypted);
  }
}
