/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.wrangler.dataset.schema;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
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
import io.cdap.wrangler.proto.schema.SchemaDescriptorType;
import io.cdap.wrangler.proto.schema.SchemaEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
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
 *
 * Schema information is stored in two tables, one for the schema metadata and one for the schema entries.
 *
 * The schema metadata table contains nine columns:
 *
 * namespace, id, name, description, created, updated, type, auto, current
 *
 * The namespace and id columns form the primary key for the metadata table. The current column points to the latest
 * schema entry version, and the auto column is used for generating version numbers for any new schema entries.
 *
 * The schema entry table contains four columns:
 *
 * namespace, id, version, schema
 *
 * The namespace, id, and version columns form the primary key, while the schema column contains the actual schema.
 */
public final class SchemaRegistry  {
  private static final String NAMESPACE_COL = "namespace";
  private static final String GENERATION_COL = "generation";
  private static final String ID_COL = "id";
  private static final String SCHEMA_MISSING = "Schema '%s' does not exist.";
  private final StructuredTable metaTable;
  private final StructuredTable entryTable;

  /**
   * Columns specific to the meta table
   */
  private static class MetaColumn {
    private static final String NAME = "name";
    private static final String DESC = "description";
    private static final String CREATED = "created";
    private static final String UPDATED = "updated";
    private static final String TYPE = "type";
    private static final String AUTO_VERSION = "auto";
    private static final String CURRENT_VERSION = "current";
  }

  /**
   * Columns specific to the entry table
   */
  private static class EntryColumn {
    private static final String VERSION = "version";
    private static final String SCHEMA = "schema";
  }

  private static final StructuredTableId META_TABLE_ID = new StructuredTableId("schema_registry_meta");
  private static final StructuredTableId ENTRY_TABLE_ID = new StructuredTableId("schema_registry_entries");
  public static final StructuredTableSpecification META_TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(META_TABLE_ID)
    .withFields(new FieldType(NAMESPACE_COL, FieldType.Type.STRING),
                new FieldType(GENERATION_COL, FieldType.Type.LONG),
                new FieldType(ID_COL, FieldType.Type.STRING),
                new FieldType(MetaColumn.NAME, FieldType.Type.STRING),
                new FieldType(MetaColumn.DESC, FieldType.Type.STRING),
                new FieldType(MetaColumn.CREATED, FieldType.Type.LONG),
                new FieldType(MetaColumn.UPDATED, FieldType.Type.LONG),
                new FieldType(MetaColumn.TYPE, FieldType.Type.STRING),
                new FieldType(MetaColumn.AUTO_VERSION, FieldType.Type.LONG),
                new FieldType(MetaColumn.CURRENT_VERSION, FieldType.Type.LONG))
    .withPrimaryKeys(NAMESPACE_COL, GENERATION_COL, ID_COL)
    .build();
  public static final StructuredTableSpecification ENTRY_TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(ENTRY_TABLE_ID)
    .withFields(new FieldType(NAMESPACE_COL, FieldType.Type.STRING),
                new FieldType(GENERATION_COL, FieldType.Type.LONG),
                new FieldType(ID_COL, FieldType.Type.STRING),
                new FieldType(EntryColumn.VERSION, FieldType.Type.LONG),
                new FieldType(EntryColumn.SCHEMA, FieldType.Type.BYTES))
    .withPrimaryKeys(NAMESPACE_COL, GENERATION_COL, ID_COL, EntryColumn.VERSION)
    .build();

  public SchemaRegistry(StructuredTable metaTable, StructuredTable entryTable) {
    this.metaTable = metaTable;
    this.entryTable = entryTable;
  }

  public static SchemaRegistry get(StructuredTableContext context) {
    try {
      StructuredTable metaTable = context.getTable(META_TABLE_ID);
      StructuredTable entryTable = context.getTable(ENTRY_TABLE_ID);
      return new SchemaRegistry(metaTable, entryTable);
    } catch (TableNotFoundException e) {
      String errorMessage = String.format(
          "System table '%s' does not exist. Please check your system environment. %s: %s",
          META_TABLE_ID.getName(), e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.USER, false, e);
    }
  }


  /**
   * Writes an entry in the schema registry. If the schema already exists, it is overwritten.
   *
   * @param schemaDescriptor information about the schema to write
   */
  public void write(SchemaDescriptor schemaDescriptor) {
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
      metaTable.upsert(toFields(builder.build()));
    } catch (IOException e) {
      String errorMessage = String.format(
          "Unable to write, failed to upsert schema metadata with ID: '%s'. %s: %s",
          schemaDescriptor.getId().getId(), e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, false, e);
    }
  }

  /**
   * Deletes the schema and all associated entries.
   *
   * @param id of the schema to delete
   */
  public void delete(NamespacedId id) {
    try {
      metaTable.delete(getMetaKey(id));
    } catch (IOException e) {
      String errorMessage = String.format(
          "Unable to delete, failed to delete schema metadata with ID: '%s'. %s: %s",
          id.getId(), e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, false, e);

    }
    Range range = Range.singleton(getMetaKey(id));
    try (CloseableIterator<StructuredRow> rowIter = entryTable.scan(range, Integer.MAX_VALUE)) {
      while (rowIter.hasNext()) {
        StructuredRow row = rowIter.next();
        Namespace namespace = new Namespace(row.getString(NAMESPACE_COL), row.getLong(GENERATION_COL));
        NamespacedId entryId = new NamespacedId(namespace, row.getString(ID_COL));
        long entryVersion = row.getLong(EntryColumn.VERSION);
        entryTable.delete(getEntryKey(entryId, entryVersion));
      }
    } catch (IOException e) {
      String errorMessage = String.format("Unable to scan entry table starting from %s. "
              + "Failed to delete schema entries with ID: '%s'. %s: %s", range, id.getId(),
          e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, false, e);
    }
  }

  /**
   * Adds a new entry to the schema.
   *
   * @param id the id of the schema to add
   * @param specification the schema to be added
   * @throws SchemaNotFoundException if the schema does not exist
   */
  public long add(NamespacedId id, byte[] specification) {
    SchemaRow existing = getSchemaRow(id);
    if (existing == null) {
      throw getProgramFailureDueToMissingSchema(String.format(SCHEMA_MISSING, id.getId()));
    }

    long version = existing.getAutoVersion() + 1;
    SchemaRow updated = SchemaRow.builder(existing)
      .setUpdated(System.currentTimeMillis() / 1000)
      .setAutoVersion(version)
      .setCurrentVersion(version)
      .build();

    // update schema information
    try {
      metaTable.upsert(toFields(updated));
    } catch (IOException e) {
      String errorMessage = String.format(
          "Unable to add, failed to upsert schema metadata with ID: '%s'. %s: %s", id.getId(),
          e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, false, e);
    }
    List<Field<?>> fields = getEntryKey(id, version);
    fields.add(Fields.bytesField(EntryColumn.SCHEMA, specification));
    try {
      entryTable.upsert(fields);
    } catch (IOException e) {
      String errorMessage = String.format(
          "Unable to add, failed to upsert schema entry with ID: '%s' and version: '%d'. %s: %s",
          id.getId(), version, e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, false, e);
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
   */
  public void remove(NamespacedId id, long version) {
    SchemaRow row = getSchemaRow(id);
    if (row == null) {
      throw getProgramFailureDueToMissingSchema(String.format(SCHEMA_MISSING, id.getId()));
    }
    try {
      entryTable.delete(getEntryKey(id, version));
    } catch (IOException e) {
      String errorMessage = String.format(
          "Unable to remove, failed to delete schema entry with ID: '%s' and version: '%d'. %s: %s",
          id.getId(), version, e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, false, e);
    }
  }

  /**
   * Checks if schema id and version combination exists in the registry.
   *
   * @param id of the schema to be checked
   * @param version version of the schema to be checked.
   * @return true if id and version matches, else false.
   * @throws SchemaNotFoundException if the schema does not exist
   */
  public boolean hasSchema(NamespacedId id, long version) {
    SchemaRow schemaRow = getSchemaRow(id);
    if (schemaRow == null) {
      throw getProgramFailureDueToMissingSchema(String.format(SCHEMA_MISSING, id.getId()));
    }
    Optional<StructuredRow> row;
    try {
      row = entryTable.read(getEntryKey(id, version));
    } catch (IOException e) {
      String errorMessage = String.format(
          "Unable to check, failed to read schema entry with ID: '%s' and version: '%d'. %s: %s",
          id.getId(), version, e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, true, e);
    }
    return row.isPresent();
  }

  /**
   * Checks if there is a schema entry, its not necessary that there are any version of schema registered.
   *
   * @param id the id of the schema to check
   * @return true if it exists, false otherwise
   */
  public boolean hasSchema(NamespacedId id) {
    Optional<StructuredRow> row = null;
    try {
      row = metaTable.read(getMetaKey(id));
    } catch (IOException e) {
      String errorMessage = String.format(
          "Unable to check, failed to read schema metadata with ID: '%s'. %s: %s", id.getId(),
          e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, true, e);
    }
    return row.isPresent();
  }

  /**
   * Return all versions of the specified schema.
   *
   * @param id the schema id
   * @return list of schema versions
   * @throws SchemaNotFoundException if the schema does not exist
   */
  public Set<Long> getVersions(NamespacedId id) {
    if (getSchemaRow(id) == null) {
      throw getProgramFailureDueToMissingSchema(String.format(SCHEMA_MISSING, id.getId()));
    }
    Range range = Range.singleton(getMetaKey(id));
    try (CloseableIterator<StructuredRow> rowIter = entryTable.scan(range, Integer.MAX_VALUE)) {
      Set<Long> versionSet = new LinkedHashSet<>();
      while (rowIter.hasNext()) {
        StructuredRow row = rowIter.next();
        versionSet.add(row.getLong(EntryColumn.VERSION));
      }
      return versionSet;
    } catch (IOException e) {
      String errorMessage = String.format("Unable to scan entry table starting from %s. "
              + "Failed to get schema versions with ID: '%s'. %s: %s", range, id.getId(),
          e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, false, e);
    }
  }

  /**
   * Get a specific version of the specified schema.
   *
   * @param id the schema id
   * @param version the entry version to get
   * @return the schema entry
   * @throws SchemaNotFoundException if the schema does not exist
   */
  public SchemaEntry getEntry(NamespacedId id, long version) {
    SchemaRow schemaRow = getSchemaRow(id);
    if (schemaRow == null) {
      throw getProgramFailureDueToMissingSchema(String.format(SCHEMA_MISSING, id.getId()));
    }
    return getEntry(schemaRow, version);
  }

  /**
   * Get the latest entry of the specified schema if it exists
   *
   * @param id the schema id
   * @return the latest entry of the specified schema
   * @throws SchemaNotFoundException if the schema or its latest entry could not be found
   */
  public SchemaEntry getEntry(NamespacedId id) {
    SchemaRow schemaRow = getSchemaRow(id);
    if (schemaRow == null) {
      throw getProgramFailureDueToMissingSchema(String.format(SCHEMA_MISSING, id.getId()));
    }
    Long version = schemaRow.getCurrentVersion();
    if (version == null) {
      return new SchemaEntry(id, schemaRow.getDescriptor().getName(), schemaRow.getDescriptor().getDescription(),
                             schemaRow.getDescriptor().getType(), Collections.emptySet(), null, null);
    }
    return getEntry(schemaRow, version);
  }

  private SchemaEntry getEntry(SchemaRow schemaRow, long version) {
    NamespacedId id = schemaRow.getDescriptor().getId();
    Optional<StructuredRow> row;
    try {
      row = entryTable.read(getEntryKey(id, version));
    } catch (IOException e) {
      String errorMessage = String.format(
          "Unable to read schema entry with ID: '%s' and version: '%d'. %s: %s", id.getId(),
          version, e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, true, e);
    }
    if (!row.isPresent()) {
      throw getProgramFailureDueToMissingSchema(
          String.format("Schema '%s' version '%d' does not exist.", id, version));
    }
    byte[] specification = row.get().getBytes(EntryColumn.SCHEMA);
    Set<Long> versions = getVersions(id);
    return new SchemaEntry(id, schemaRow.getDescriptor().getName(), schemaRow.getDescriptor().getDescription(),
                           schemaRow.getDescriptor().getType(), versions, specification,
                           schemaRow.getCurrentVersion());
  }

  private static ProgramFailureException getProgramFailureDueToMissingSchema(String errorMessage) {
    return ErrorUtils.getProgramFailureException(
        new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
        ErrorType.USER, false, new SchemaNotFoundException(errorMessage));
  }

  @Nullable
  private SchemaRow getSchemaRow(NamespacedId id) {
    Optional<StructuredRow> row;
    try {
      row = metaTable.read(getMetaKey(id));
    } catch (IOException e) {
      String errorMessage = String.format("Unable to read schema metadata with ID: '%s'. %s: %s",
          id.getId(), e.getClass().getName(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorMessage, errorMessage,
          ErrorType.SYSTEM, true, e);
    }
    return row.map(this::fromRow).orElse(null);
  }

  private List<Field<?>> getMetaKey(NamespacedId id) {
    List<Field<?>> fields = new ArrayList<>(2);
    fields.add(Fields.stringField(NAMESPACE_COL, id.getNamespace().getName()));
    fields.add(Fields.longField(GENERATION_COL, id.getNamespace().getGeneration()));
    fields.add(Fields.stringField(ID_COL, id.getId()));
    return fields;
  }

  private List<Field<?>> getEntryKey(NamespacedId schemaId, long version) {
    List<Field<?>> fields = new ArrayList<>(3);
    fields.add(Fields.stringField(NAMESPACE_COL, schemaId.getNamespace().getName()));
    fields.add(Fields.longField(GENERATION_COL, schemaId.getNamespace().getGeneration()));
    fields.add(Fields.stringField(ID_COL, schemaId.getId()));
    fields.add(Fields.longField(EntryColumn.VERSION, version));
    return fields;
  }

  private List<Field<?>> toFields(SchemaRow schemaRow) {
    List<Field<?>> fields = new ArrayList<>(9);
    fields.add(Fields.stringField(NAMESPACE_COL, schemaRow.getDescriptor().getId().getNamespace().getName()));
    fields.add(Fields.longField(GENERATION_COL, schemaRow.getDescriptor().getId().getNamespace().getGeneration()));
    fields.add(Fields.stringField(ID_COL, schemaRow.getDescriptor().getId().getId()));
    fields.add(Fields.stringField(MetaColumn.NAME, schemaRow.getDescriptor().getName()));
    fields.add(Fields.stringField(MetaColumn.DESC, schemaRow.getDescriptor().getDescription()));
    fields.add(Fields.stringField(MetaColumn.TYPE, schemaRow.getDescriptor().getType().name()));
    fields.add(Fields.longField(MetaColumn.CREATED, schemaRow.getCreated()));
    fields.add(Fields.longField(MetaColumn.UPDATED, schemaRow.getUpdated()));
    fields.add(Fields.longField(MetaColumn.AUTO_VERSION, schemaRow.getAutoVersion()));
    if (schemaRow.getCurrentVersion() != null) {
      fields.add(Fields.longField(MetaColumn.CURRENT_VERSION, schemaRow.getCurrentVersion()));
    }
    return fields;
  }

  private SchemaRow fromRow(StructuredRow row) {
    Namespace namespace = new Namespace(row.getString(NAMESPACE_COL), row.getLong(GENERATION_COL));
    NamespacedId id = new NamespacedId(namespace, row.getString(ID_COL));
    SchemaDescriptor descriptor = new SchemaDescriptor(id, row.getString(MetaColumn.NAME),
                                                       row.getString(MetaColumn.DESC),
                                                       SchemaDescriptorType.valueOf(row.getString(MetaColumn.TYPE)));
    return SchemaRow.builder(descriptor)
      .setCreated(row.getLong(MetaColumn.CREATED))
      .setUpdated(row.getLong(MetaColumn.UPDATED))
      .setAutoVersion(row.getLong(MetaColumn.AUTO_VERSION))
      .setCurrentVersion(row.getLong(MetaColumn.CURRENT_VERSION))
      .build();
  }

}
