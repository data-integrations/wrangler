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

package io.cdap.wrangler.dataset.connections;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.Predicate;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * This class {@link ConnectionStore} manages all the connections defined.
 * It manages the lifecycle of the {@link Connection} including all CRUD operations.
 *
 * The store is backed by a table with namespace, id, type, name, description, properties, created, and updated columns.
 * The primary key is the namespace and id.
 */
@Deprecated
public class ConnectionStore {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String NAMESPACE_COL = "namespace";
  private static final String GENERATION_COL = "generation";
  private static final String ID_COL = "id";
  private static final String TYPE_COL = "type";
  private static final String NAME_COL = "name";
  private static final String DESC_COL = "description";
  private static final String PROPERTIES_COL = "properties";
  private static final String CREATED_COL = "created";
  private static final String UPDATED_COL = "updated";
  private static final String PRECONFIGURED_COL = "preconfigured";
  private static final StructuredTableId TABLE_ID = new StructuredTableId("connections");
  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(TABLE_ID)
    .withFields(new FieldType(NAMESPACE_COL, FieldType.Type.STRING),
                new FieldType(GENERATION_COL, FieldType.Type.LONG),
                new FieldType(ID_COL, FieldType.Type.STRING),
                new FieldType(TYPE_COL, FieldType.Type.STRING),
                new FieldType(NAME_COL, FieldType.Type.STRING),
                new FieldType(DESC_COL, FieldType.Type.STRING),
                new FieldType(PROPERTIES_COL, FieldType.Type.STRING),
                new FieldType(CREATED_COL, FieldType.Type.LONG),
                new FieldType(UPDATED_COL, FieldType.Type.LONG),
                new FieldType(PRECONFIGURED_COL, FieldType.Type.STRING))
    .withPrimaryKeys(NAMESPACE_COL, GENERATION_COL, ID_COL)
    .build();

  private final StructuredTable table;

  private ConnectionStore(StructuredTable table) {
    this.table = table;
  }

  public static ConnectionStore get(StructuredTableContext context) {
    try {
      StructuredTable table = context.getTable(TABLE_ID);
      return new ConnectionStore(table);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(String.format(
        "System table '%s' does not exist. Please check your system environment.", TABLE_ID.getName()), e);
    }
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
  public NamespacedId create(Namespace namespace,
                             ConnectionMeta meta) throws ConnectionAlreadyExistsException, IOException {
    return create(namespace, meta, false);
  }

  public NamespacedId create(Namespace namespace, ConnectionMeta meta,
                             boolean preconfigured) throws ConnectionAlreadyExistsException, IOException {
    NamespacedId id = new NamespacedId(namespace, getConnectionId(meta.getName()));
    Connection existing = read(id);
    if (existing != null) {
      throw new ConnectionAlreadyExistsException(
        String.format("Connection named '%s' with id '%s' already exists.", meta.getName(), id.getId()));
    }

    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    Connection connection = Connection.builder(id, meta)
      .setCreated(now)
      .setUpdated(now)
      .setPreconfigured(preconfigured)
      .build();
    table.upsert(toFields(connection, namespace.getGeneration()));
    return id;
  }

  /**
   * Get the specified connection.
   *
   * @param id the id of the connection
   * @return the connection information
   * @throws ConnectionNotFoundException if the connection does not exist
   */
  public Connection get(NamespacedId id) throws ConnectionNotFoundException, IOException {
    Connection existing = read(id);
    if (existing == null) {
      throw new ConnectionNotFoundException(String.format("Connection '%s' does not exist", id.getId()));
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
  public void update(NamespacedId id, ConnectionMeta meta) throws ConnectionNotFoundException, IOException {
    Connection existing = get(id);

    Connection updated = Connection.builder(id, meta)
      .setCreated(existing.getCreated())
      .setUpdated(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
      .build();
    table.upsert(toFields(updated, id.getNamespace().getGeneration()));
  }

  /**
   * Deletes the specified connection.
   *
   * @param id the connection to delete
   */
  public void delete(NamespacedId id) throws IOException {
    table.delete(getKey(id));
  }

  /**
   * Returns true if connection identified by connectionName already exists.
   */
  public boolean connectionExists(Namespace namespace, String connectionName) throws IOException {
    return read(new NamespacedId(namespace, getConnectionId(connectionName))) != null;
  }

  /**
   * Scans the namespace to list all the keys applying the filter.
   *
   * @param filter to be applied on the data being returned.
   * @return List of connections
   */
  public List<Connection> list(Namespace namespace, Predicate<Connection> filter) throws IOException {
    List<Field<?>> key = new ArrayList<>(2);
    key.add(Fields.stringField(NAMESPACE_COL, namespace.getName()));
    key.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    Range range = Range.singleton(key);
    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
      List<Connection> result = new ArrayList<>();
      while (rowIter.hasNext()) {
        StructuredRow row = rowIter.next();
        Connection connection = fromRow(row);
        if (filter.apply(connection)) {
          result.add(connection);
        }
      }
      return result;
    }
  }

  /**
   * Delete all connection with the namespace and generation id
   */
  public void deleteAll(NamespaceSummary namespace) throws IOException {
    List<Field<?>> key = new ArrayList<>(2);
    key.add(Fields.stringField(NAMESPACE_COL, namespace.getName()));
    key.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    Range range = Range.singleton(key);
    table.deleteAll(range);
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
  private Connection read(NamespacedId id) throws IOException {
    Optional<StructuredRow> row = table.read(getKey(id));
    return row.map(this::fromRow).orElse(null);
  }

  private List<Field<?>> getKey(NamespacedId id) {
    List<Field<?>> keyFields = new ArrayList<>(2);
    keyFields.add(Fields.stringField(NAMESPACE_COL, id.getNamespace().getName()));
    keyFields.add(Fields.longField(GENERATION_COL, id.getNamespace().getGeneration()));
    keyFields.add(Fields.stringField(ID_COL, id.getId()));
    return keyFields;
  }

  private List<Field<?>> toFields(Connection connection, long generation) {
    List<Field<?>> fields = new ArrayList<>(8);
    fields.add(Fields.stringField(NAMESPACE_COL, connection.getNamespace()));
    fields.add(Fields.longField(GENERATION_COL, generation));
    fields.add(Fields.stringField(ID_COL, connection.getId()));
    fields.add(Fields.stringField(TYPE_COL, connection.getType().name()));
    fields.add(Fields.stringField(NAME_COL, connection.getName()));
    fields.add(Fields.stringField(DESC_COL, connection.getDescription()));
    fields.add(Fields.stringField(PROPERTIES_COL, GSON.toJson(connection.getProperties())));
    fields.add(Fields.longField(CREATED_COL, connection.getCreated()));
    fields.add(Fields.longField(UPDATED_COL, connection.getUpdated()));
    fields.add(Fields.stringField(PRECONFIGURED_COL, String.valueOf(connection.isPreconfigured())));

    return fields;
  }

  private Connection fromRow(StructuredRow row) {
    Namespace namespace = new Namespace(row.getString(NAMESPACE_COL), row.getLong(GENERATION_COL));
    return Connection.builder(new NamespacedId(namespace, row.getString(ID_COL)))
      .setType(ConnectionType.valueOf(row.getString(TYPE_COL)))
      .setName(row.getString(NAME_COL))
      .setDescription(row.getString(DESC_COL))
      .setProperties(GSON.fromJson(row.getString(PROPERTIES_COL), MAP_TYPE))
      .setCreated(row.getLong(CREATED_COL))
      .setUpdated(row.getLong(UPDATED_COL))
      .setPreconfigured(Boolean.valueOf(row.getString(PRECONFIGURED_COL)))
      .build();
  }
}
