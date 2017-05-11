package co.cask.wrangler.dataset.connections;

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
  public void testEncryptionAndDecreption() throws Exception {
    try
    {
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
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }

}