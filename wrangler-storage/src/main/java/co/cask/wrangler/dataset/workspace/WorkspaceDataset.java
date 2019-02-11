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

package co.cask.wrangler.dataset.workspace;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableNotFoundException;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.FieldType;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.proto.WorkspaceIdentifier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Workspace store for workspaces and a special row for the DirectiveConfig, which allows admins to configure a
 * directive blacklist and to alias directives to other names.
 *
 * A workspace contains a data sample, metadata about the workspace, and a set of directives that can be used to
 * process the data sample. A workspace is tagged with a scope, which can be used to group workspaces together.
 * It also stores a map of properties, which are connection specific properties that are used to generate the
 * pipeline source configuration when a pipeline is created from a workspace.
 *
 * The dataset is stored in a single table with columns:
 *
 * namespace, id, name, type, scope, created, updated, properties, data, and request
 */
public class WorkspaceDataset {
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(Request.class, new RequestDeserializer())
    .create();
  private static final String NAMESPACE_COL = "namespace";
  private static final String ID_COL = "id";
  private static final String NAME_COL = "name";
  private static final String TYPE_COL = "type";
  private static final String SCOPE_COL = "scope";
  private static final String CREATED_COL = "created";
  private static final String UPDATED_COL = "updated";
  private static final String PROPERTIES_COL = "properties";
  private static final String DATA_COL = "data";
  private static final String REQUEST_COL = "request";
  private static final StructuredTableId TABLE_ID = new StructuredTableId("workspaces");
  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(TABLE_ID)
    .withFields(new FieldType(NAMESPACE_COL, FieldType.Type.STRING),
                new FieldType(ID_COL, FieldType.Type.STRING),
                new FieldType(NAME_COL, FieldType.Type.STRING),
                new FieldType(TYPE_COL, FieldType.Type.STRING),
                new FieldType(SCOPE_COL, FieldType.Type.STRING),
                new FieldType(CREATED_COL, FieldType.Type.LONG),
                new FieldType(UPDATED_COL, FieldType.Type.LONG),
                new FieldType(PROPERTIES_COL, FieldType.Type.STRING),
                new FieldType(DATA_COL, FieldType.Type.BYTES),
                new FieldType(REQUEST_COL, FieldType.Type.STRING))
    .withPrimaryKeys(NAMESPACE_COL, ID_COL)
    .build();
  public static final String DEFAULT_SCOPE = "default";
  private final StructuredTable table;

  public WorkspaceDataset(StructuredTable table) {
    this.table = table;
  }

  public static WorkspaceDataset get(StructuredTableContext context) {
    try {
      StructuredTable table = context.getTable(TABLE_ID);
      return new WorkspaceDataset(table);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(String.format(
        "System table '%s' does not exist. Please check your system environment.", TABLE_ID.getName()), e);
    }
  }

  /**
   * Creates a workspace if it does not already exist, or update an existing workspace if it does.
   *
   * @param meta the workspace metadata
   */
  public void writeWorkspaceMeta(WorkspaceMeta meta) throws IOException {
    Workspace existing = readWorkspace(meta);
    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    Workspace.Builder updated = Workspace.builder(meta, meta.getName());
    if (existing != null) {
      updated.setCreated(existing.getCreated())
        .setData(existing.getData())
        .setRequest(existing.getRequest());
    } else {
      updated.setCreated(now);
    }
    updated.setUpdated(now)
      .setScope(meta.getScope())
      .setProperties(meta.getProperties())
      .setType(meta.getType());
    table.upsert(toFields(updated.build()));
  }

  /**
   * Get information about the workspace.
   *
   * @param id the workspace id
   * @return information about the workspace
   * @throws WorkspaceNotFoundException if the workspace does not exist
   */
  public Workspace getWorkspace(NamespacedId id) throws WorkspaceNotFoundException, IOException {
    Workspace workspace = readWorkspace(id);
    if (workspace == null) {
      throw new WorkspaceNotFoundException(String.format("Workspace '%s' does not exist.", id.getId()));
    }
    return workspace;
  }

  /**
   * Checks if a workspace exists.
   *
   * @param id of the workspace to be checked for.
   * @return true if workspace exists, false otherwise.
   */
  public boolean hasWorkspace(NamespacedId id) throws IOException {
    Optional<StructuredRow> row = table.read(getKey(id));
    return row.isPresent();
  }

  /**
   * Lists all the workspaces registered for a scope.
   *
   * @return List of workspaces.
   */
  public List<WorkspaceIdentifier> listWorkspaces(String namespace, String scope) throws IOException {
    List<WorkspaceIdentifier> values = new ArrayList<>();
    Range range = Range.singleton(Collections.singletonList(Fields.stringField(NAMESPACE_COL, namespace)));
    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
      while (rowIter.hasNext()) {
        StructuredRow row = rowIter.next();
        Workspace workspace = readWorkspace(row);
        if (scope.equals(workspace.getScope())) {
          values.add(new WorkspaceIdentifier(workspace.getId(), workspace.getName()));
        }
      }
    }
    return values;
  }

  /**
   * Update the properties of the specified workspace.
   *
   * @param id the workspace id
   * @param properties the properties to update
   * @throws WorkspaceNotFoundException if the workspace does not exist
   */
  public void updateWorkspaceProperties(NamespacedId id,
                                        Map<String, String> properties) throws WorkspaceNotFoundException, IOException {
    Workspace existing = getWorkspace(id);
    Workspace updated = Workspace.builder(existing)
      .setProperties(properties)
      .setUpdated(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
      .build();
    table.upsert(toFields(updated));
  }

  /**
   * Update the directive execution request for the specified workspace.
   *
   * @param id the workspace id
   * @param request the directive execution request
   * @throws WorkspaceNotFoundException if the workspace does not exist
   */
  public void updateWorkspaceRequest(NamespacedId id, Request request) throws WorkspaceNotFoundException, IOException {
    Workspace existing = getWorkspace(id);
    Workspace updated = Workspace.builder(existing)
      .setRequest(request)
      .setUpdated(System.currentTimeMillis() / 1000)
      .build();
    table.upsert(toFields(updated));
  }

  /**
   * Update the sample data for the specified workspace.
   *
   * @param id the workspace id
   * @param data the sample data
   * @throws WorkspaceNotFoundException if the workspace does not exist
   */
  public void updateWorkspaceData(NamespacedId id, DataType dataType,
                                  byte[] data) throws WorkspaceNotFoundException, IOException {
    Workspace existing = getWorkspace(id);
    Workspace updated = Workspace.builder(existing)
      .setType(dataType)
      .setData(data)
      .setUpdated(System.currentTimeMillis() / 1000)
      .build();
    table.upsert(toFields(updated));
  }

  /**
   * Deletes the workspace.
   *
   * @param id to be deleted.
   */
  public void deleteWorkspace(NamespacedId id) throws IOException {
    table.delete(getKey(id));
  }

  /**
   * Deletes a workspaces that have the specified scope.
   *
   * TODO: (CDAP-14692) make sure scope is indexed so this doesn't require a full table scan
   *
   * @param scope to be deleted
   * @return number of workspaces deleted
   */
  public int deleteScope(String namespace, String scope) throws IOException {
    Range range = Range.singleton(Collections.singletonList(Fields.stringField(NAMESPACE_COL, namespace)));
    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
      int count = 0;
      while (rowIter.hasNext()) {
        StructuredRow row = rowIter.next();
        Workspace workspace = readWorkspace(row);
        if (scope.equals(workspace.getScope())) {
          deleteWorkspace(workspace);
          count++;
        }
      }
      return count;
    }
  }

  private List<Field<?>> toFields(Workspace workspace) {
    List<Field<?>> fields = new ArrayList<>(10);
    fields.add(Fields.stringField(NAMESPACE_COL, workspace.getNamespace()));
    fields.add(Fields.stringField(ID_COL, workspace.getId()));
    fields.add(Fields.stringField(NAME_COL, workspace.getName()));
    fields.add(Fields.stringField(SCOPE_COL, workspace.getScope()));
    fields.add(Fields.stringField(TYPE_COL, workspace.getType().name()));
    fields.add(Fields.stringField(PROPERTIES_COL, GSON.toJson(workspace.getProperties())));
    fields.add(Fields.longField(CREATED_COL, workspace.getCreated()));
    fields.add(Fields.longField(UPDATED_COL, workspace.getUpdated()));
    Request request = workspace.getRequest();
    if (request != null) {
      fields.add(Fields.stringField(REQUEST_COL, GSON.toJson(request)));
    }
    byte[] data = workspace.getData();
    if (data != null) {
      fields.add(Fields.bytesField(DATA_COL, data));
    }
    return fields;
  }

  @Nullable
  private Workspace readWorkspace(NamespacedId id) throws IOException {
    Optional<StructuredRow> row = table.read(getKey(id));
    return row.map(this::readWorkspace).orElse(null);
  }

  private Workspace readWorkspace(StructuredRow row) {
    NamespacedId id = new NamespacedId(row.getString(NAMESPACE_COL), row.getString(ID_COL));

    String propertiesStr = row.getString(PROPERTIES_COL);
    Map<String, String> properties = propertiesStr == null || propertiesStr.isEmpty() ?
      Collections.emptyMap() : GSON.fromJson(propertiesStr, MAP_TYPE);
    String requestStr = row.getString(REQUEST_COL);
    Request request = requestStr == null || requestStr.isEmpty() ?
      null : GSON.fromJson(requestStr, Request.class);

    return Workspace.builder(id, row.getString(NAME_COL))
      .setCreated(row.getLong(CREATED_COL))
      .setUpdated(row.getLong(UPDATED_COL))
      .setData(row.getBytes(DATA_COL))
      .setRequest(request)
      .setScope(row.getString(SCOPE_COL))
      .setType(DataType.valueOf(row.getString(TYPE_COL)))
      .setProperties(properties)
      .build();
  }

  private List<Field<?>> getKey(NamespacedId id) {
    List<Field<?>> keyFields = new ArrayList<>();
    keyFields.add(Fields.stringField(NAMESPACE_COL, id.getNamespace()));
    keyFields.add(Fields.stringField(ID_COL, id.getId()));
    return keyFields;
  }

}
