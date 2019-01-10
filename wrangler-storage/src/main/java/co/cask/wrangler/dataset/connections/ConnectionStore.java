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

package co.cask.wrangler.dataset.connections;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.dataset.NamespacedKeys;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionMeta;
import co.cask.wrangler.proto.connection.ConnectionType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * This class {@link ConnectionStore} manages all the connections defined.
 * It manages the lifecycle of the {@link Connection} including all CRUD operations.
 *
 * Following is the example for usage.
 *
 * <code>
 *  ConnectionStore store = new ConnectionStore(table);
 *  String id = store.create(connection);
 *  if(!store.hasKey(id)) {
 *    Connection connection = store.value(id);
 *    connection.setUpdated(System.currentTimeMillis());
 *    store.update(id, connection);
 *  }
 *  Connection newConnection = store.clone(id);
 *  List<Connection> s = store.list();
 * </code>
 */
public class ConnectionStore {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String TYPE_COL = "type";
  private static final String NAME_COL = "name";
  private static final String DESC_COL = "description";
  private static final String PROPERTIES_COL = "properties";
  private static final String CREATED_COL = "created";
  private static final String UPDATED_COL = "updated";
  private final Table table;

  public ConnectionStore(Table table) {
    this.table = table;
  }

  /**
   * Creates an entry in the {@link ConnectionStore} for object {@link Connection}.
   *
   * This method creates the id and returns if after successfully updating the store.
   *
   * @param meta the metadata of the connection create
   * @return id of the connection stored
   * @throws ConnectionAlreadyExistsException if the connection already exists
   */
  public NamespacedId create(String namespace, ConnectionMeta meta) throws ConnectionAlreadyExistsException {
    NamespacedId id = NamespacedId.of(namespace, getConnectionId(meta.getName()));
    Connection existing = read(id);
    if (existing != null) {
      throw new ConnectionAlreadyExistsException(
        String.format("Connection named '%s' with id '%s' already exists.", meta.getName(), id));
    }

    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    Connection connection = Connection.builder(id, meta)
      .setCreated(now)
      .setUpdated(now)
      .build();
    table.put(toPut(connection));
    return connection.getId();
  }

  /**
   * Get the specified connection.
   *
   * @param id the id of the connection
   * @return the connection information
   * @throws ConnectionNotFoundException if the connection does not exist
   */
  public Connection get(NamespacedId id) throws ConnectionNotFoundException {
    Connection existing = read(id);
    if (existing == null) {
      throw new ConnectionNotFoundException(String.format("Connection '%s' does not exist", id));
    }
    return existing;
  }

  /**
   * Updates an existing connection in the store.
   *
   * @param id the id of the object to be update
   * @param meta metadata to update
   * @throws ConnectionNotFoundException if the specified connection does not exist
   */
  public void update(NamespacedId id, ConnectionMeta meta) throws ConnectionNotFoundException {
    Connection existing = get(id);

    Connection updated = Connection.builder(id, meta)
      .setCreated(existing.getCreated())
      .setUpdated(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
      .build();
    table.put(toPut(updated));
  }

  /**
   * Deletes the specified connection.
   *
   * @param id the connection to delete
   */
  public void delete(NamespacedId id) {
    table.delete(NamespacedKeys.getRowKey(id));
  }

  /**
   * Returns true if connection identified by connectionName already exists.
   */
  public boolean connectionExists(String namespace, String connectionName) {
    return read(NamespacedId.of(namespace, getConnectionId(connectionName))) != null;
  }

  /**
   * Scans the namespace to list all the keys applying the filter.
   *
   * @param filter to be applied on the data being returned.
   * @return List of connections
   */
  public List<Connection> list(String namespace, Predicate<Connection> filter) {
    List<Connection> result = new ArrayList<>();
    try (Scanner scan = table.scan(NamespacedKeys.getScan(namespace))) {
      Row row;
      while ((row = scan.next()) != null) {
        Connection connection = fromRow(row);
        if (filter.apply(connection)) {
          result.add(connection);
        }
      }
    }
    return result;
  }

  /**
   * Get connection id for the given connection name
   *
   * @param name name of the connection.
   * @return connection id.
   */
  public static String getConnectionId(String name) {
    name = name.trim();
    // Lower case columns
    name = name.toLowerCase();
    // Filtering unwanted characters
    name = name.replaceAll("[^a-zA-Z0-9_]", "_");
    return name;
  }

  @Nullable
  private Connection read(NamespacedId id) {
    Row row = table.get(NamespacedKeys.getRowKey(id));
    if (row.isEmpty()) {
      return null;
    }
    return fromRow(row);
  }

  private Put toPut(Connection connection) {
    Put put = new Put(NamespacedKeys.getRowKey(connection.getId()));
    put.add(TYPE_COL, connection.getType().name());
    put.add(NAME_COL, connection.getName());
    put.add(DESC_COL, connection.getDescription());
    put.add(PROPERTIES_COL, GSON.toJson(connection.getProperties()));
    put.add(CREATED_COL, connection.getCreated());
    put.add(UPDATED_COL, connection.getUpdated());
    return put;
  }

  private Connection fromRow(Row row) {
    return Connection.builder(NamespacedKeys.fromRowKey(row.getRow()))
      .setType(ConnectionType.valueOf(row.getString(TYPE_COL)))
      .setName(row.getString(NAME_COL))
      .setDescription(row.getString(DESC_COL))
      .setProperties(GSON.fromJson(row.getString(PROPERTIES_COL), MAP_TYPE))
      .setCreated(row.getLong(CREATED_COL))
      .setUpdated(row.getLong(UPDATED_COL))
      .build();
  }
}
