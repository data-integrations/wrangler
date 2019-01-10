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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.api.DirectiveConfig;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.proto.WorkspaceIdentifier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Workspace store for workspaces and a special row for the DirectiveConfig, which allows admins to configure a
 * directive blacklist and to alias directives to other names.
 * TODO: (CDAP-14619) check if the DirectiveConfig is used by anything/anyone. If so, see if it can be moved to app
 *   configuration instead of stored in a one row table.
 *
 * A workspace contains a data sample, metadata about the workspace, and a set of directives that can be used to
 * process the data sample. A workspace is tagged with a scope, which can be used to group workspaces together.
 * It also stores a map of properties, which are connection specific properties that are used to generate the
 * pipeline source configuration when a pipeline is created from a workspace.
 */
public class WorkspaceDataset {
  public static final String DATASET_NAME = "workspaces";
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(Request.class, new RequestDeserializer())
    .create();
  private final Table table;

  public static final String DEFAULT_SCOPE = "default";
  private static final byte[] CONFIG_KEY = Bytes.toBytes("__config__");
  private static final byte[] CONFIG_COL = Bytes.toBytes("__ws__");

  private static final String DATA_COL = "data";
  private static final String SCOPE_COL = "scope";
  private static final String NAME_COL = "name";
  private static final String TYPE_COL = "type";
  private static final String CREATED_COL = "created";
  private static final String UPDATED_COL = "updated";
  private static final String PROPERTIES_COL = "properties";
  private static final String REQUEST_COL = "request";

  public WorkspaceDataset(Table table) {
    this.table = table;
  }

  /**
   * Creates a workspace if it does not already exist, or update an existing workspace if it does.
   *
   * @param meta the workspace metadata
   */
  public void writeWorkspaceMeta(WorkspaceMeta meta) {
    Workspace existing = readWorkspace(meta.getId());
    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    Workspace.Builder updated = Workspace.builder(meta.getId(), meta.getName());
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
    table.put(toPut(updated.build()));
  }

  /**
   * Get information about the workspace.
   *
   * @param id the workspace id
   * @return information about the workspace
   * @throws WorkspaceNotFoundException if the workspace does not exist
   */
  public Workspace getWorkspace(String id) throws WorkspaceNotFoundException {
    Workspace workspace = readWorkspace(id);
    if (workspace == null) {
      throw new WorkspaceNotFoundException(String.format("Workspace '%s' does not exist.", id));
    }
    return workspace;
  }

  /**
   * Checks if a workspace exists.
   *
   * @param id of the workspace to be checked for.
   * @return true if workspace exists, false otherwise.
   */
  public boolean hasWorkspace(String id) {
    co.cask.cdap.api.dataset.table.Row row = table.get(Bytes.toBytes(id));
    return !row.isEmpty();
  }

  /**
   * Lists all the workspaces registered for a scope.
   *
   * @return List of workspaces.
   */
  public List<WorkspaceIdentifier> listWorkspaces(String scope) {
    List<WorkspaceIdentifier> values = new ArrayList<>();
    co.cask.cdap.api.dataset.table.Row row;
    try (Scanner scanner = table.scan(null, null)) {
      while ((row = scanner.next()) != null) {
        byte[] key = row.getRow();
        String id = Bytes.toString(key);
        if (excludedKey(id)) {
          continue;
        }
        byte[] scopeBytes = row.get(SCOPE_COL);
        String scopeStr = Bytes.toString(scopeBytes);
        if (!scope.equals(scopeStr)) {
          continue;
        }
        byte[] name = row.get(NAME_COL);
        values.add(new WorkspaceIdentifier(id, Bytes.toString(name)));
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
  public void updateWorkspaceProperties(String id, Map<String, String> properties) throws WorkspaceNotFoundException {
    Workspace existing = getWorkspace(id);
    Workspace updated = Workspace.builder(existing)
      .setProperties(properties)
      .setUpdated(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))
      .build();
    table.put(toPut(updated));
  }

  /**
   * Update the directive execution request for the specified workspace.
   *
   * @param id the workspace id
   * @param request the directive execution request
   * @throws WorkspaceNotFoundException if the workspace does not exist
   */
  public void updateWorkspaceRequest(String id, Request request) throws WorkspaceNotFoundException {
    Workspace existing = getWorkspace(id);
    Workspace updated = Workspace.builder(existing)
      .setRequest(request)
      .setUpdated(System.currentTimeMillis() / 1000)
      .build();
    table.put(toPut(updated));
  }

  /**
   * Update the sample data for the specified workspace.
   *
   * @param id the workspace id
   * @param data the sample data
   * @throws WorkspaceNotFoundException if the workspace does not exist
   */
  public void updateWorkspaceData(String id, DataType dataType, byte[] data) throws WorkspaceNotFoundException {
    Workspace existing = getWorkspace(id);
    Workspace updated = Workspace.builder(existing)
      .setType(dataType)
      .setData(data)
      .setUpdated(System.currentTimeMillis() / 1000)
      .build();
    table.put(toPut(updated));
  }

  /**
   * Deletes the workspace.
   *
   * @param id to be deleted.
   */
  public void deleteWorkspace(String id) {
    table.delete(Bytes.toBytes(id));
  }

  /**
   * Deletes a workspaces that have the specified scope.
   *
   * TODO: (CDAP-14692) make sure scope is indexed so this doesn't require a full table scan
   *
   * @param scope to be deleted
   * @return number of workspaces deleted
   */
  public int deleteScope(String scope) {
    int count = 0;
    co.cask.cdap.api.dataset.table.Row row;
    try (Scanner scanner = table.scan(null, null)) {
      while ((row = scanner.next()) != null) {
        byte[] key = row.getRow();
        String id = Bytes.toString(key);
        if (excludedKey(id)) {
          continue;
        }
        byte[] groupBytes = row.get(SCOPE_COL);
        String groupStr = Bytes.toString(groupBytes);
        if (scope.equals(groupStr)) {
          deleteWorkspace(id);
          count = count + 1;
        }
      }
    }
    return count;
  }

  public void updateConfig(DirectiveConfig config) {
    byte[] bytes = Bytes.toBytes(GSON.toJson(config));
    table.put(CONFIG_KEY, CONFIG_COL, bytes);
  }

  public DirectiveConfig getConfig() {
    byte[] bytes = table.get(CONFIG_KEY, CONFIG_COL);
    String json;
    if (bytes == null) {
      json = "{}";
    } else {
      json = Bytes.toString(bytes);
    }
    return GSON.fromJson(json, DirectiveConfig.class);
  }

  public String getConfigString() {
    byte[] bytes = table.get(CONFIG_KEY, CONFIG_COL);
    if (bytes == null) {
      return "{}";
    }
    return Bytes.toString(bytes);
  }

  private boolean excludedKey(String id) {
    return id.equalsIgnoreCase(Bytes.toString(CONFIG_KEY));
  }

  private Put toPut(Workspace workspace) {
    Put put = new Put(workspace.getId());
    put.add(NAME_COL, workspace.getName());
    put.add(SCOPE_COL, workspace.getScope());
    put.add(TYPE_COL, workspace.getType().name());
    put.add(PROPERTIES_COL, GSON.toJson(workspace.getProperties()));
    put.add(CREATED_COL, workspace.getCreated());
    put.add(UPDATED_COL, workspace.getUpdated());
    if (workspace.getRequest() != null) {
      put.add(REQUEST_COL, GSON.toJson(workspace.getRequest()));
    }
    if (workspace.getData() != null) {
      put.add(DATA_COL, workspace.getData());
    }
    return put;
  }

  @Nullable
  private Workspace readWorkspace(String id) {
    Row row = table.get(Bytes.toBytes(id));
    if (row.isEmpty()) {
      return null;
    }

    String propertiesStr = row.getString(PROPERTIES_COL);
    Map<String, String> properties = propertiesStr == null || propertiesStr.isEmpty() ?
      Collections.emptyMap() : GSON.fromJson(propertiesStr, MAP_TYPE);
    String requestStr = row.getString(REQUEST_COL);
    Request request = requestStr == null || requestStr.isEmpty() ?
      null : GSON.fromJson(requestStr, Request.class);

    return Workspace.builder(id, row.getString(NAME_COL))
      .setCreated(row.getLong(CREATED_COL))
      .setUpdated(row.getLong(UPDATED_COL))
      .setData(row.get(DATA_COL))
      .setRequest(request)
      .setScope(row.getString(SCOPE_COL))
      .setType(DataType.valueOf(row.getString(TYPE_COL)))
      .setProperties(properties)
      .build();
  }

}
