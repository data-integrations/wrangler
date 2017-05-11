package co.cask.wrangler.dataset.connections;

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
      System.err.println(aesKey.getFormat());
      System.err.println(new String(aesKey.getEncoded()));
      Cipher cipher = Cipher.getInstance("AES");
      // encrypt the text
      cipher.init(Cipher.ENCRYPT_MODE, aesKey);
      byte[] encrypted = cipher.doFinal(text.getBytes());
      System.err.println(new String(encrypted));
      // decrypt the text
      cipher.init(Cipher.DECRYPT_MODE, aesKey);
      String decrypted = new String(cipher.doFinal(encrypted));
      System.err.println(decrypted);
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }

}