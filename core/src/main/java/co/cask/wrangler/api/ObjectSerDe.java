package co.cask.wrangler.api;

import co.cask.wrangler.api.annotations.PublicEvolving;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Class description here.
 */
@PublicEvolving
public class ObjectSerDe<T> {

  public byte[] toByteArray(T object) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(object);
      out.flush();
      return bos.toByteArray();
    } finally {
      try {
        bos.close();
      } catch (IOException ex) {
        // ignore close exception
      }
    }
  }

  public T toObject(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    ObjectInput in = null;
    try {
      in = new ObjectInputStream(bis);
      T object = (T) in.readObject();
      return object;
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException ex) {
        // ignore close exception
      }
    }
  }

}
