/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.wrangler.proto.schema.SchemaEntry;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class {@link SchemaRegistry} is responsible for managing the schema registry store.
 *
 * A schema is uniquely identified by an ID and contains other metadata about the schema, such as a name, description,
 * type, etc. By itself, a schema does not contain an actual schema object. It is more like a group of schema entries.
 *
 * A schema entry contains the actual schema bytes and is uniquely identified by the schema it belongs to and a
 * version number. A schema entry is added to a schema and can also be removed. In addition, each schema keeps track
 * of the latest schema entry.
 */
public final class SchemaRegistry  {
  public static final String DATASET_NAME = "schemaRegistry";
  // Table in which all the information of the schema is stored.
  private final Table table;

  public SchemaRegistry(Table table) {
    this.table = table;
  }

  /**
   * Writes an entry in the schema registry. If the schema already exists, it is overwritten.
   *
   * @param schemaDescriptor information about the schema to write
   * @throws SchemaRegistryException if there was an error reading from or writing to the storage system
   */
  public void write(SchemaDescriptor schemaDescriptor) throws SchemaRegistryException {
    SchemaRow.Builder builder = SchemaRow.builder(schemaDescriptor);

    long now = System.currentTimeMillis() / 1000;
    SchemaRow existing = getSchemaRow(schemaDescriptor.getId());
    if (existing == null) {
      builder.setCreated(now)
        .setUpdated(now)
        .setAutoVersion(0L);
    } else {
      builder.setCreated(existing.getCreated())
        .setUpdated(now)
        .setAutoVersion(existing.getAutoVersion())
        .setCurrentVersion(existing.getCurrentVersion());
    }

    try {
      table.put(builder.build().toPut());
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to create schema descriptor '%s'. %s", schemaDescriptor.getId(), e.getMessage()));
    }
  }

  /**
   * Deletes the schema and all associated entries.
   *
   * @param id of the schema.
   * @throws SchemaRegistryException if there was an error reading from or writing to the storage system
   */
  public void delete(String id) throws SchemaRegistryException {
    try {
      table.delete(Bytes.toBytes(id));
    } catch (DataSetException e) {
      throw new SchemaRegistryException(String.format("Unable to delete schema. %s", e.getMessage()));
    }
  }

  /**
   * Adds a new entry to the schema.
   *
   * @param id of the schema.
   * @param specification of the schema to be added.
   * @throws SchemaNotFoundException if the schema does not exist
   * @throws SchemaRegistryException if there was an error reading from or writing to the storage system
   */
  public long add(String id, byte[] specification) throws SchemaRegistryException {
    SchemaRow existing = getSchemaRow(id);
    if (existing == null) {
      throw new SchemaNotFoundException(String.format("Schema '%s' does not exist.", id));
    }

    long version = existing.getAutoVersion() + 1;
    SchemaRow updated = SchemaRow.builder(existing)
      .setUpdated(System.currentTimeMillis() / 1000)
      .setAutoVersion(version)
      .setCurrentVersion(version)
      .build();

    try {
      // update schema information
      table.put(updated.toPut());
      // add schema entry
      Put entry = new Put(id);
      entry.add(toVersionColumn(version), specification);
      table.put(entry);
    } catch (DataSetException e) {
      throw new SchemaRegistryException(String.format("Unable to add entry to schema '%s'. %s", id, e.getMessage()));
    }
    return version;
  }

  /**
   * Deletes a specified version of the schema.
   *
   * TODO: (CDAP-14661) update latest version pointer if the latest version was removed
   *
   * @param id of the schema to be deleted.
   * @param version of the schema to be deleted.
   * @throws SchemaNotFoundException if the schema does not exist
   * @throws SchemaRegistryException if there was an error reading from or writing to the storage system
   */
  public void remove(String id, long version) throws SchemaRegistryException {
    try {
      SchemaRow row = getSchemaRow(id);
      if (row == null) {
        throw new SchemaNotFoundException(String.format("Schema '%s' does not exist.", id));
      }
      table.delete(Bytes.toBytes(id), toVersionColumn(version));
    } catch (DataSetException e) {
      throw new SchemaRegistryException(String.format("Unable to delete schema '%s'. %s", id, e.getMessage()));
    }
  }

  /**
   * Checks if schema id and version combination exists in the registry.
   *
   * @param id of the schema to be checked
   * @param version version of the schema to be checked.
   * @return true if id and version matches, else false.
   * @throws SchemaNotFoundException if the schema does not exist
   * @throws SchemaRegistryException if there was an error reading from or writing to the storage system
   */
  public boolean hasSchema(String id, long version) throws SchemaRegistryException {
    try {
      Row row = table.get(Bytes.toBytes(id));
      if (row.isEmpty()) {
        throw new SchemaNotFoundException(String.format("Schema '%s' does not exist", id));
      }
      return row.getColumns().keySet().contains(toVersionColumn(version));
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check if schema '%s' version '%d'. %s", id, version, e.getMessage()));
    }
  }

  /**
   * Checks if there is a schema entry, its not necessary that there are any version of schema registered.
   *
   * @param id of the schema to be checked for.
   * @return true if it exists, false otherwise.
   * @throws SchemaRegistryException if there was an error reading from or writing to the storage system
   */
  public boolean hasSchema(String id) throws SchemaRegistryException {
    try {
      Row row = table.get(Bytes.toBytes(id));
      return !row.isEmpty();
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check if schema '%s' exists. %s", id, e.getMessage()));
    }
  }

  /**
   * Return all versions of the specified schema.
   *
   * @param id the schema id
   * @return list of schema versions
   * @throws SchemaNotFoundException if the schema does not exist
   * @throws SchemaRegistryException if there was an error reading from or writing to the storage system
   */
  public Set<Long> getVersions(String id) throws SchemaRegistryException {
    try {
      Row row = table.get(Bytes.toBytes(id));
      if (row.isEmpty()) {
        throw new SchemaNotFoundException(String.format("Schema '%s' does not exist.", id));
      }
      Set<byte[]> versions = row.getColumns().keySet();
      Set<Long> versionSet = new LinkedHashSet<>();
      for (byte[] version : versions) {
        String v = new String(version, StandardCharsets.UTF_8);
        int idx = v.indexOf("ver:");
        if (idx != -1) {
          String number = v.substring(idx + 4);
          versionSet.add(Long.parseLong(number));
        }
      }
      return versionSet;
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check what versions of schema '%s' exist. %s", id, e.getMessage()));
    }
  }

  /**
   * Get a specific version of the specified schema.
   *
   * @param id the schema id
   * @param version the entry version to get
   * @return the schema entry
   * @throws SchemaNotFoundException if the schema does not exist
   * @throws SchemaRegistryException if there was an error reading from or writing to the storage system
   */
  public SchemaEntry getEntry(String id, long version) throws SchemaRegistryException {
    try {
      SchemaRow schemaRow = getSchemaRow(id);
      if (schemaRow == null) {
        throw new SchemaNotFoundException(String.format("Schema '%s' does not exist.", id));
      }
      byte[] specification = table.get(Bytes.toBytes(id), toVersionColumn(version));
      if (specification == null) {
        throw new SchemaNotFoundException(String.format("Schema '%s' version '%d' does not exist.", id, version));
      }
      Set<Long> versions = getVersions(id);
      return new SchemaEntry(id, schemaRow.getName(), schemaRow.getDescription(), schemaRow.getType(),
                             versions, specification, schemaRow.getCurrentVersion());
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check if schema '%s' version '%d' exists. '%s'", id, version, e.getMessage()));
    }
  }

  /**
   * Get the latest entry of the specified schema if it exists
   *
   * @param id the schema id
   * @return the latest entry of the specified schema
   * @throws SchemaNotFoundException if the schema or its latest entry could not be found
   * @throws SchemaRegistryException if there was an error reading from or writing to the storage system
   */
  public SchemaEntry getEntry(String id) throws SchemaRegistryException {
    try {
      SchemaRow schemaRow = getSchemaRow(id);
      if (schemaRow == null) {
        throw new SchemaNotFoundException(String.format("Schema '%s' does not exist.", id));
      }
      Long version = schemaRow.getCurrentVersion();
      if (version == null) {
        return new SchemaEntry(id, schemaRow.getName(), schemaRow.getDescription(), schemaRow.getType(),
                               Collections.emptySet(), null, null);
      }
      byte[] specification = table.get(Bytes.toBytes(id), toVersionColumn(version));
      if (specification == null) {
        throw new SchemaNotFoundException(String.format("Schema '%s' version '%d' does not exist.", id, version));
      }
      Set<Long> versions = getVersions(id);
      return new SchemaEntry(id, schemaRow.getName(), schemaRow.getDescription(), schemaRow.getType(),
                             versions, specification, schemaRow.getCurrentVersion());
    } catch (DataSetException e) {
      throw new SchemaRegistryException(
        String.format("Unable to check if the latest version of schema '%s' exists. '%s'", id, e.getMessage()));
    }
  }
  
  private byte[] toVersionColumn(long version) {
    String ver = String.format("ver:%d", version);
    return ver.getBytes(StandardCharsets.UTF_8);
  }

  @Nullable
  private SchemaRow getSchemaRow(String id) {
    Row row = table.get(Bytes.toBytes(id));
    return row.isEmpty() ? null : SchemaRow.fromRow(row);
  }
}
