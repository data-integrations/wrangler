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

package co.cask.wrangler.dataset.schema;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Charsets;
import com.google.common.collect.HashBasedTable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class {@link SchemaRegistry} is a {@link co.cask.cdap.api.dataset.Dataset} that is responsible
 * for managing the schema registry store.
 */
public class SchemaRegistry extends AbstractDataset {
  private final Gson gson;
  private final Table table;
  private static final byte[] NAME_COL             = Bytes.toBytes("name");
  private static final byte[] DESC_COL             = Bytes.toBytes("description");
  private static final byte[] CREATED_COL          = Bytes.toBytes("created");
  private static final byte[] UPDATED_COL          = Bytes.toBytes("updated");
  private static final byte[] TYPE_COL             = Bytes.toBytes("type");
  private static final byte[] AUTO_VERSION_COL     = Bytes.toBytes("auto");
  private static final byte[] ACTIVE_VERSION_COL   = Bytes.toBytes("current");

  /**
   * Schema Entry/
   */
  public static final class SchemaEntry {
    private final String id;
    private final String name;
    private final String description;
    private final SchemaDescriptorType type;
    private final Set<Long> versions;
    private final byte[] specification;
    private final long current;

    public SchemaEntry(String id, String name, String description, SchemaDescriptorType type,
                       Set<Long> versions, byte[] specification, long current) {
      this.id = id;
      this.name = name;
      this.description = description;
      this.type = type;
      this.versions = versions;
      this.specification = specification;
      this.current = current;
    }

    public String getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }

    public SchemaDescriptorType getType() {
      return type;
    }

    public Set<Long> getVersions() {
      return versions;
    }

    public byte[] getSpecification() {
      return specification;
    }

    public long getCurrent() {
      return current;
    }
  }

  public SchemaRegistry(DatasetSpecification specification,
                        @EmbeddedDataset("schema") Table table) {
    super(specification.getName(), table);
    this.gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
    this.table = table;
  }

  /**
   * Creates an entry in the schema registry.
   *
   * @param id of the schema.
   * @param name of the schema.
   * @param description for the schema.
   */
  public void create(String id, String name, String description, SchemaDescriptorType type)
    throws SchemaRegistryException {
    // Schema registry columns.
    byte[][] columns = new byte[][] {
      NAME_COL, DESC_COL, CREATED_COL, TYPE_COL, AUTO_VERSION_COL, ACTIVE_VERSION_COL
    };

    byte[][] data = new byte[][] {
      Bytes.toBytes(name),
      Bytes.toBytes(description),
      Bytes.toBytes(System.currentTimeMillis() / 1000),
      Bytes.toBytes(type.toString()),
      Bytes.toBytes(1L),
      Bytes.toBytes(1L)
    };
    try {
      table.put(toIdKey(id), columns, data);
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to create schema descriptor '%s'. '%s'",
                      id, e.getMessage())
      );
    }
  }

  /**
   * Deletes the entrie schema definition for the id specified.
   *
   * @param id of the schema.
   */
  public void delete(String id) throws SchemaRegistryException {
    try {
      table.delete(toIdKey(id));
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to delete schema '%s'",
                      e.getMessage())
      );
    }
  }

  /**
   * Adds a new version of schema.
   *
   * @param id of the schema.
   * @param specification
   * @throws SchemaRegistryException
   */
  public long add(String id,  byte[] specification) throws SchemaRegistryException {
    long version = getNextVersion(id);
    // Schema registry columns.
    byte[][] columns = new byte[][] {
      toVersionColumn(version), UPDATED_COL, ACTIVE_VERSION_COL
    };

    byte[][] data = new byte[][] {
      specification,
      Bytes.toBytes(System.currentTimeMillis() / 1000),
      Bytes.toBytes(version)
    };
    try {
      table.put(toIdKey(id), columns, data);
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to add schema for id '%s'. '%s'",
                      id, e.getMessage())
      );
    }
    return version;
  }

  /**
   * Deletes a specified version of the schema.
   *
   * @param id of the schema to be deleted.
   * @param version of the schema to be deleted.
   */
  public void remove(String id, long version) throws SchemaRegistryException {
    try {
      table.delete(toIdKey(id), toVersionColumn(version));
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to delete schema '%s'",
                      e.getMessage())
      );
    }
  }

  /**
   * Checks if schema id and version combination exists in the registry.
   *
   * @param id of the schema to be checked
   * @param version version of the schema to be checked.
   * @return true if id and version matches, else false.
   */
  public boolean hasSchema(String id, long version) throws SchemaRegistryException {
    try {
      Row row = table.get(toIdKey(id));
      if (row.isEmpty()) {
        return false;
      }
      if (row.getColumns().keySet().contains(toVersionColumn(version))) {
        return true;
      }
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check if schema id and version exists. '%s'",
                      e.getMessage())
      );
    }
    return false;
  }

  public Set<Long> getVersions(String id) throws SchemaRegistryException {
    try {
      Row row = table.get(toIdKey(id));
      if (row.isEmpty()) {
        return new HashSet<>();
      }
      Set<byte[]> versions = row.getColumns().keySet();
      Set<Long> versionSet = new HashSet<>();
      for (byte[] version : versions) {
        String v = new String(version, Charsets.UTF_8);
        int idx = v.indexOf("ver:");
        if (idx != -1) {
          String number = v.substring(idx + 4);
          versionSet.add(Long.parseLong(number));
        }
      }
      return versionSet;
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check if schema id and version exists. '%s'",
                      e.getMessage())
      );
    }
  }

  public String getName(String id) throws SchemaRegistryException {
    try {
      return Bytes.toString(table.get(toIdKey(id), NAME_COL));
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to retrieve name field for id '%s'. '%s'",
                      id, e.getMessage())
      );
    }
  }

  public String getDescription(String id) throws SchemaRegistryException {
    try {
      return Bytes.toString(table.get(toIdKey(id), DESC_COL));
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to retrieve description field for id '%s'. '%s'",
                      id, e.getMessage())
      );
    }
  }

  public SchemaDescriptorType getType(String id) throws SchemaRegistryException {
    try {
      String type = Bytes.toString(table.get(toIdKey(id), TYPE_COL));
      return SchemaDescriptorType.fromString(type);
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to retrieve description field for id '%s'. '%s'",
                      id, e.getMessage())
      );
    }
  }

  public byte[] getSchema(String id, long version) throws SchemaRegistryException {
    try {
      return table.get(toIdKey(id), toVersionColumn(version));
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check if schema id and version exists. '%s'",
                      e.getMessage())
      );
    }
  }

  public SchemaEntry get(String id, long version) throws SchemaRegistryException {
    try {
      String name = getName(id);
      String description = getDescription(id);
      SchemaDescriptorType type = getType(id);
      Set<Long> versions = getVersions(id);
      byte[] specification = table.get(toIdKey(id), toVersionColumn(version));
      long current = getCurrentVersion(id);
      return new SchemaEntry(id, name, description, type, versions, specification, current);
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check if schema id and version exists. '%s'",
                      e.getMessage())
      );
    }
  }

  public SchemaEntry get(String id) throws SchemaRegistryException {
    try {
      long version = getCurrentVersion(id);
      String name = getName(id);
      String description = getDescription(id);
      SchemaDescriptorType type = getType(id);
      Set<Long> versions = getVersions(id);
      byte[] specification = table.get(toIdKey(id), toVersionColumn(version));
      long current = getCurrentVersion(id);
      return new SchemaEntry(id, name, description, type, versions, specification, current);
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check if schema id and version exists. '%s'",
                      e.getMessage())
      );
    }
  }

  public byte[] getSchema(String id) throws SchemaRegistryException {
    long version = getCurrentVersion(id);
    return getSchema(id, version);
  }

  public long getCurrentVersion(String id) throws SchemaRegistryException {
    try {
      byte[] bytes = table.get(toIdKey(id), ACTIVE_VERSION_COL);
      return Bytes.toLong(bytes);
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to get current version of schema id '%s'. '%s'",
                      id, e.getMessage())
      );
    }
  }

  public com.google.common.collect.Table<String, String, String> list() throws SchemaRegistryException {
    com.google.common.collect.Table<String, String, String> result = HashBasedTable.create();

    Row row;
    try (Scanner scanner = table.scan(null, null)) {
      while((row = scanner.next()) != null) {
        byte[] key = row.getRow();
        String id = Bytes.toString(key);
        Map<byte[], byte[]> columns = row.getColumns();
        for (Map.Entry<byte[], byte[]> column : columns.entrySet()) {
          String name = Bytes.toString(column.getKey());
          String value = Bytes.toString(column.getValue());
          result.put(id, name, value);
        }
      }
      return result;
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to list schemas. ", e.getMessage())
      );
    }
  }

  private long getNextVersion(String id) throws SchemaRegistryException {
    try {
      long nextVersion = table.incrementAndGet(toIdKey(id), AUTO_VERSION_COL, 1);
      return nextVersion;
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to get next version of schema id '%s'. '%s'",
                      id, e.getMessage())
      );
    }
  }

  private byte[] toIdKey(String id) {
    return id.getBytes(Charsets.UTF_8);
  }

  private byte[] toVersionColumn(long version) {
    String ver = String.format("ver:%d", version);
    return ver.getBytes(Charsets.UTF_8);
  }


}
