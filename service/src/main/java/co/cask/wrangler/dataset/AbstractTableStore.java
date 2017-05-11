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

package co.cask.wrangler.dataset;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.List;

/**
 * This abstract class {@link AbstractTableStore} is an abstraction over the generic table with ability
 * to store multiple type of objects. Any type of structure can be easily stored using this class. In order
 * to store different type of object within this store following needs to be done.
 *
 * <ul>
 *   <li>
 *      Extend from this class.
 *      <code>
 *        public class ConnectionStore extends AbstractStore<Connection> {
 *          ....
 *        }
 *      </code>
 *      {code}
 *   </li>
 *   <li>
 *     Implement a few abstract methods relevant to the data being stored.
 *     <code>
 *       @Override protected String getNamespace() { ... }
 *       @Override protected String getDelimiter() { ... }
 *       @Override public String create(Connection connection) { ... }
 *       @Override public void update(String id, Connection connection) { ... }
 *       @Override public Connection clone(String id) { ... }
 *     </code>
 *   </li>
 * </ul>
 */
public abstract class AbstractTableStore<T> {
  // Table in which all the object data is store.
  private final Table table;

  // Gson handler for converting object into JSON.
  private final Gson gson;

  // Column to which the data should be written.
  private final byte[] column;

  protected AbstractTableStore(Table table, byte[] column) {
    this.table = table;
    this.column = column;
    gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  }

  /**
   * Converts object of type T to JSON string.
   *
   * @param object to be converted
   * @return string representation of object.
   */
  protected String toJson(T object) {
    return gson.toJson(object);
  }

  /**
   * Converts object of type T to byte array.
   *
   * @param object to be converted
   * @return byte array representing JSON.
   */
  protected byte[] toJsonBytes(T object) {
    return Bytes.toBytes(toJson(object));
  }

  /**
   * Converts from JSON string to object of type T.
   *
   * @param json string to be converted to object.
   * @return instance of type T object.
   */
  protected T fromJson(String json) {
    return (T) gson.fromJson(json, new TypeToken<T>(){}.getType());
  }

  /**
   * Converts from JSON byte array to object of type T.
   * @param json byte array to be converted to object.
   * @return instance of type T object.
   */
  protected T fromJson(byte[] json) {
    return fromJson(Bytes.toString(json));
  }

  /**
   * Given a key, returns the object of type T from the table.
   *
   * @param key to be used to retrieve the object.
   * @return instance of type T object if found, else returns null.
   */
  protected T getObject(byte[] key) {
    byte[] bytes = table.get(key, column);
    if (bytes != null) {
      return fromJson(bytes);
    }
    return null;
  }

  /**
   * Given a key of type string, returns the object of type T from the table.
   *
   * @param key to be used to retrieve the object.
   * @return instance of type T object if found, else returns null.
   */
  protected T getObject(String key) {
    return getObject(Bytes.toBytes(key));
  }

  /**
   * Adds the object of type T to the table using the key represented as byte array.
   *
   * @param key to be used to store the object.
   * @param object instance of the object to be stored.
   */
  protected void putObject(byte[] key, T object) {
    byte[] bytes = toJsonBytes(object);
    table.put(key, column, bytes);
  }

  /**
   * Generates a composite key that is a combination of namespace, delimiter and object key.
   *
   * @param key specifies the key to be used to generate the composite key.
   * @return composite key.
   */
  protected byte[] generateKey(String key) {
    String compositeKey = String.format("%s%s%s", getNamespace(), getDelimiter(), key);
    return Bytes.toBytes(compositeKey);
  }

  /**
   * Mangles the name.
   *
   * @param name to be mangled.
   * @return mangled name.
   */
  protected String mangle(String name) {
    name = name.trim();
    // Lower case columns
    name = name.toLowerCase();
    // Filtering unwanted characters
    name = name.replaceAll("[^a-zA-Z0-9_]", "_");
    return name;
  }

  /**
   * @return current time in seconds.
   */
  protected long now() {
    return System.currentTimeMillis() / 1000;
  }

  /**
   * Updates the table given an id and connection instance.
   *
   * @param id of the key.
   * @param connection object to be stored.
   */
  protected void updateTable(String id, T connection) {
    byte[] bytes = toJsonBytes(connection);
    table.put(generateKey(id), column, bytes);
  }

  /**
   * @return Abstract method implemented by the extending class to provide the key namespace.
   */
  protected abstract String getNamespace();

  /**
   * @return Abstract method implemented by the extending class to provide the delimiter for the composite key.
   */
  protected abstract String getDelimiter();

  /**
   * Scans the namespace to list all the keys applying the filter.
   *
   * @param filter to be applied on the data being returned.
   * @return List of objects of type T.
   */
  public List<T> scan(Predicate<T> filter) {
    List<T> result = new ArrayList<>();
    byte[] startKey = Bytes.toBytes(getNamespace());
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
    try (Scanner scan = table.scan(startKey, stopKey)) {
      Row next;
      while ((next = scan.next()) != null) {
        byte[] bytes = next.get(column);
        T connection = fromJson(bytes);
        if (filter != null && filter.apply(connection)) {
          result.add(connection);
        }
      }
    }
    return result;
  }

  /**
   * Checks if the key is present in the store.
   *
   * @param id of the key to be checked.
   * @return true if present, false otherwise.
   */
  public boolean hasKey(String id) {
    byte[] bytes = generateKey(id);
    Row row = table.get(bytes);
    if(row.isEmpty()) {
      return false;
    }
    return true;
  }

  /**
   * Deletes the object with key id from the table in the namespace.
   *
   * @param id of the object to be deleted.
   */
  public void delete(String id) {
    table.delete(generateKey(id));
  }

  /**
   * Returns the instance of stored object of type T from the store.
   *
   * @param id of the object to be retrieved.
   * @return instance of object of type T if present, else false.
   */
  public T get(String id) {
    return getObject(generateKey(id));
  }

  /**
   * Creates an instance of the object of type T in the store.
   * If the object already exits in the store, which is determined by looking at the key,
   * then an exception is thrown.
   *
   * @param object to be created in the store.
   * @return Id of the object that was created.
   */
  public abstract String create(T object);

  /**
   * Updates the already existing object in the object store.
   *
   * @param id of the object to be updated.
   * @param object itself to be updated.
   */
  public abstract void update(String id, T object);

  /**
   * Clones the object with the id specified.
   * NOTE: The object is not updated in the object store.
   *
   * @param id of the object to be cloned.
   * @return instance of the newly created object.
   */
  public abstract T clone(String id);
}
