/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.utils;

import co.cask.wrangler.api.annotations.PublicEvolving;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * This class allows one to serialize the object of T into bytes.
 *
 * @param <T> type of object to serialize
 */
@PublicEvolving
public final class ObjectSerDe<T> {

  /**
   * Converts an object of type T into bytes.
   *
   * @param object to be serialized into bytes.
   * @return byte array of serialized object.
   */
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

  /**
   * Converts a serialized object byte array back into object.
   *
   * @param bytes to be converted to object of type T.
   * @return an instance of object deserialized from the byte array.
   * @see ObjectSerDe#toByteArray(Object)
   */
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
