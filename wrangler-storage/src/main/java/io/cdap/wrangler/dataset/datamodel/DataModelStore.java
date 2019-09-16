/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.wrangler.dataset.datamodel;

import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
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
import io.cdap.wrangler.dataset.schema.SchemaFileNotFoundException;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.datamodel.DataModelInfo;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModel;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModel.Discriminator;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModel.MetaData;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModelDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * DataModelStore is responsible for managing the storage of all system and client defined
 * {@link JsonSchemaDataModel}.
 */
public final class DataModelStore {

  private static final String NAMESPACE_COL = "namespace";
  private static final String GENERATION_COL = "generation";
  private static final String ID_COL = "id";
  private static final String SCHEMA_COL = "schema";
  private static final String DISCRIMINATOR_COL = "discriminator";
  private static final String METADATA_COL = "metadata";
  private static final Gson GSON = new Gson();
  private static final StructuredTableId TABLE_ID = new StructuredTableId("datamodels");
  private final StructuredTable table;

  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
      .withId(TABLE_ID)
      .withFields(new FieldType(NAMESPACE_COL, FieldType.Type.STRING),
          new FieldType(GENERATION_COL, FieldType.Type.LONG),
          new FieldType(ID_COL, FieldType.Type.STRING),
          new FieldType(SCHEMA_COL, FieldType.Type.STRING),
          new FieldType(DISCRIMINATOR_COL, FieldType.Type.STRING),
          new FieldType(METADATA_COL, FieldType.Type.STRING))
      .withPrimaryKeys(NAMESPACE_COL, GENERATION_COL, ID_COL)
      .build();


  public DataModelStore(StructuredTable table) {
    this.table = table;
  }

  public static DataModelStore get(StructuredTableContext context) {
    try {
      StructuredTable table = context.getTable(TABLE_ID);
      return new DataModelStore(table);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(String.format(
          "System table '%s' does not exist. Please check your system environment.",
          TABLE_ID.getName()), e);
    }
  }

  /**
   * Reads a data model from storage
   *
   * @param id the {@JsonSchemaDataModelDescriptor} id
   * @return the {@JsonSchemaDataModelDescriptor}
   */
  public JsonSchemaDataModelDescriptor readDataModelDescriptor(NamespacedId id) throws IOException {
    Optional<StructuredRow> row = table.read(getSchemaKey(id));
    JsonSchemaDataModelDescriptor descriptor = row.map(this::readDataModelDescriptor)
        .orElse(null);
    if (descriptor == null) {
      String message = String.format("Schema file '%s' does not exist.", id);
      throw new SchemaFileNotFoundException(message);
    }
    return descriptor;
  }

  /**
   * Adds a schema file to storage if it does not already exist, or updates an existing
   * definition if it does exist.
   *
   * @param descriptor the schema definition to persist.
   */
  public void addOrUpdateDataModel(JsonSchemaDataModelDescriptor descriptor)
      throws IOException {
    table.upsert(getFields(descriptor));
  }

  /**
   * Checks if the schema definition exists.
   *
   * @param id of the schema definition.
   * @return true if the schema definition exists, false if does not exist.
   */
  public boolean hasDataModel(NamespacedId id) throws IOException {
    Optional<StructuredRow> row = table.read(getSchemaKey(id));
    return row.isPresent();
  }

  /**
   * Checks whether store has any {@SchemaFile}
   *
   * @return if a Schema File exists in the store.
   */
  public boolean empty(Namespace namespace) throws IOException {
    final int limit = 1;
    Range range = Range.singleton(getNamespaceKey(namespace));
    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, limit)) {
      return !rowIter.hasNext();
    }
  }

  /**
   * Lists all of the schema definitions contained within store.
   *
   * @return List of schema definition descriptors.
   */
  public List<DataModelInfo<JsonSchemaDataModel.Standard>> listDataModels(Namespace namespace) throws IOException {
    List<DataModelInfo<JsonSchemaDataModel.Standard>> dataModels = new ArrayList<>();
    Range range = Range.singleton(getNamespaceKey(namespace));
    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
      while (rowIter.hasNext()) {
        StructuredRow row = rowIter.next();
        MetaData metaData = GSON.fromJson(row.getString(METADATA_COL), MetaData.class);
        dataModels.add(new DataModelInfo<>(row.getString(ID_COL), metaData.getStandard(), metaData.getVersion()));
      }
    }
    return dataModels;
  }

  /**
   * Lists all of the models within a data model.
   * @param id of the data model
   * @return List of model names
   * @throws IOException
   */
  public List<String> listModels(NamespacedId id) throws IOException {
    List<String> models = new ArrayList<>();
    JsonSchemaDataModelDescriptor descriptor = readDataModelDescriptor(id);
    JsonSchemaDataModel dataModel = descriptor.getDataModel();
    if (dataModel == null) {
      return null;
    }

    Discriminator discriminator = dataModel.getDiscriminator();
    if (discriminator != null && discriminator.getMapping() != null) {
      models.addAll(discriminator.getMapping().keySet());
    }
    return models;
  }

  /**
   * Retrieves the model definition for a given model.
   * @param id of the data model
   * @param name of the model
   * @return the model definition
   * @throws IOException
   */
  @Nullable
  public Object getModel(NamespacedId id, String name) throws IOException {
    JsonSchemaDataModelDescriptor descriptor = readDataModelDescriptor(id);
    Map<String, Object> definitions = descriptor.getDataModel().getDefinitions();
    if (definitions == null || !definitions.containsKey(name)) {
      return null;
    }
    return definitions.get(name);
  }

  /**
   * Deletes the schema definition from storage
   *
   * @param id of schema definition to be deleted.
   */
  public void deleteDataModel(NamespacedId id) throws IOException {
    table.delete(getSchemaKey(id));
  }

  private JsonSchemaDataModelDescriptor readDataModelDescriptor(StructuredRow row) {
    NamespacedId id = new NamespacedId(new Namespace(row.getString(NAMESPACE_COL), row.getLong(GENERATION_COL)),
        row.getString(ID_COL));
    return JsonSchemaDataModelDescriptor.builder()
        .setId(id)
        .setJsonSchemaDataModel(GSON.fromJson(row.getString(SCHEMA_COL), JsonSchemaDataModel.class))
        .build();
  }

  private List<Field<?>> getFields(JsonSchemaDataModelDescriptor descriptor) {
    JsonSchemaDataModel def = descriptor.getDataModel();
    List<Field<?>> fields = new ArrayList<>(4);
    fields.add(Fields.stringField(NAMESPACE_COL, descriptor.getNamespacedId().getNamespace().getName()));
    fields.add(Fields.longField(GENERATION_COL, descriptor.getNamespacedId().getNamespace().getGeneration()));
    fields.add(Fields.stringField(ID_COL, descriptor.getNamespacedId().getId()));
    fields.add(Fields.stringField(SCHEMA_COL, GSON.toJson(def)));
    fields.add(Fields.stringField(DISCRIMINATOR_COL, GSON.toJson(def.getDiscriminator())));
    fields.add(Fields.stringField(METADATA_COL, GSON.toJson(def.getMetadata())));
    return fields;
  }

  private List<Field<?>> getSchemaKey(NamespacedId id) {
    List<Field<?>> fields = new ArrayList<>(3);
    fields.add(Fields.stringField(NAMESPACE_COL, id.getNamespace().getName()));
    fields.add(Fields.longField(GENERATION_COL, id.getNamespace().getGeneration()));
    fields.add(Fields.stringField(ID_COL, id.getId()));
    return fields;
  }

  private List<Field<?>> getNamespaceKey(Namespace namespace) {
    List<Field<?>> fields = new ArrayList<>(2);
    fields.add(Fields.stringField(NAMESPACE_COL, namespace.getName()));
    fields.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    return fields;
  }
}
