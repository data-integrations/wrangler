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

package co.cask.wrangler.dataset.workspace;

import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.ReadWrite;
import co.cask.cdap.api.annotation.WriteOnly;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.dataset.KeyNotFoundException;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class WorkspaceDataset extends AbstractDataset {
  private static final Logger LOG = LoggerFactory.getLogger(WorkspaceDataset.class);
  private final Table table;
  private final Gson gson;
  public static final byte[] DATA_COL       = Bytes.toBytes("data");
  public static final byte[] TYPE_COL       = Bytes.toBytes("type");
  public static final byte[] CREATED_COL    = Bytes.toBytes("created");
  public static final byte[] UPDATED_COL    = Bytes.toBytes("updated");
  public static final byte[] PROPERTIES_COL = Bytes.toBytes("properties");
  public static final byte[] REQUEST_COL    = Bytes.toBytes("request");

  public WorkspaceDataset(DatasetSpecification specification,
                          @EmbeddedDataset("workspace") Table table){
    super(specification.getName(), table);
    this.table = table;
    this.gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  }

  /**
   * Creates a workspace meta entry, with default type as {@link DataType#BINARY} and empty properties.
   *
   * @param workspace name of workspace.
   * @throws WorkspaceException thrown when there is issue creating workspace.
   */
  @WriteOnly
  public void createWorkspaceMeta(@Nullable String workspace) throws WorkspaceException {
    createWorkspaceMeta(workspace, DataType.BINARY);
  }

  /**
   * Creates a workspace meta entry, with the type specified and empty properties.
   *
   * @param workspace Name of workspace to be created.
   * @param type of data in workspace.
   * @throws WorkspaceException thrown when issue creating workspace meta entry.
   */
  @WriteOnly
  public void createWorkspaceMeta(@Nullable String workspace, DataType type) throws WorkspaceException {
    createWorkspaceMeta(workspace, type, new HashMap<String, String>());
  }

  /**
   * Creates a workspace meta entry with the type specified and properties.
   *
   * @param workspace Name of workspace to be created.
   * @param type of data stored in workspace.
   * @param properties associated with workspace.
   * @throws WorkspaceException thrown when issue creating workspace meta entry.
   */
  @WriteOnly
  public void createWorkspaceMeta(@Nullable String workspace, DataType type,
                     Map<String, String> properties) throws WorkspaceException {
    if (workspace == null || workspace.isEmpty()) {
      throw new WorkspaceException("Workspace name cannot be empty or null");
    }

    byte[][] columns = new byte[][] {
     CREATED_COL, TYPE_COL, PROPERTIES_COL
    };

    byte[][] data = new byte[][] {
      Bytes.toBytes(System.currentTimeMillis() / 1000),
      Bytes.toBytes(type.getType()),
      toJsonBytes(properties)
    };

    try {
      table.put(toKey(workspace), columns, data);
    } catch (DataSetException e) {
      throw new WorkspaceException(
        String.format("Unable to create workspace '%s'",
                      e.getMessage())
      );
    }
  }

  /**
   * Lists all the workspaces registered.
   *
   * @return List of workspaces.
   * @throws WorkspaceException throw if there is issue listing workspaces.
   */
  @ReadOnly
  public List<String> getWorkspaces() throws WorkspaceException {
    List<String> values = new ArrayList<>();
    Row row;
    try (Scanner scanner = table.scan(null, null)) {
      while((row = scanner.next()) != null) {
        byte[] key = row.getRow();
        values.add(Bytes.toString(key));
      }
    } catch (DataSetException e) {
      throw new WorkspaceException(
        String.format("Unable to list workspace. ", e.getMessage())
      );
    }
    return values;
  }

  /**
   * Deletes the workspace.
   *
   * @param workspace to be deleted.
   * @throws WorkspaceException thrown if there is issue deleting workspace.
   */
  @WriteOnly
  public void deleteWorkspace(String workspace) throws WorkspaceException {
    try {
      table.delete(toKey(workspace));
    } catch (DataSetException e){
      throw new WorkspaceException(
        String.format("Failed to delete workspace '%s'. %s", workspace, e.getMessage())
      );
    }
  }

  @WriteOnly
  public void writeToWorkspace(String workspace, byte[] key, DataType type, byte[] data)
    throws WorkspaceException {
    byte[][] columns = new byte[][] {
      UPDATED_COL, TYPE_COL, key
    };

    byte[][] bytes = new byte[][] {
      Bytes.toBytes(System.currentTimeMillis() / 1000),
      Bytes.toBytes(type.getType()),
      data
    };

    try {
      table.put(toKey(workspace), columns, bytes);
    } catch (DataSetException e) {
      throw new WorkspaceException(
        String.format("Unable to create workspace '%s'",
                      e.getMessage())
      );
    }
  }

  @WriteOnly
  public void updateWorkspace(String workspace, byte[] key, byte[] data)
    throws WorkspaceException {
    byte[][] columns = new byte[][] {
      UPDATED_COL, key
    };

    byte[][] bytes = new byte[][] {
      Bytes.toBytes(System.currentTimeMillis() / 1000),
      data
    };

    try {
      table.put(toKey(workspace), columns, bytes);
    } catch (DataSetException e) {
      throw new WorkspaceException(
        String.format("Unable to create workspace '%s'",
                      e.getMessage())
      );
    }
  }

  @WriteOnly
  public void updateWorkspace(String workspace, byte[] key, String data) throws WorkspaceException {
    updateWorkspace(workspace, key, data.getBytes(Charsets.UTF_8));
  }

  @WriteOnly
  public void writeProperties(String workspace, Map<String, String> properties) throws WorkspaceException {
    byte[] bytes = toJsonBytes(properties);
    updateWorkspace(workspace, PROPERTIES_COL, bytes);
  }

  @ReadWrite
  public void updateProperty(String workspace, String key, String value) throws KeyNotFoundException, WorkspaceException {
    byte[] bytes = getData(workspace, Bytes.toBytes(key));
    Map<String, String> properties = fromJsonBytes(bytes);
    properties.put(key, value);
    updateWorkspace(workspace, PROPERTIES_COL, toJsonBytes(properties));
  }

  /**
   * Retrieves the data from the workspace provided the key.
   *
   * @param workspace name of the workspace.
   * @param key the key to be retrieved.
   * @return if key is found, returns the data, else throws a {@link KeyNotFoundException}.
   */
  @ReadOnly
  public byte[] getData(String workspace, byte[] key) throws KeyNotFoundException, WorkspaceException {
    byte[] bytes = table.get(toKey(workspace), key);
    if(bytes == null) {
      throw new KeyNotFoundException(
        String.format("Workspace '%s' doesn't have key '%s'.", workspace, Bytes.toShort(key))
      );
    }
    return bytes;
  }

  @ReadOnly
  public <T> T getData(String workspace, byte[] key, DataType type) throws KeyNotFoundException, WorkspaceException {
    byte[] bytes = table.get(toKey(workspace), key);
    if(bytes == null) {
      throw new KeyNotFoundException(
        String.format("Workspace '%s' doesn't have key '%s'.", workspace, Bytes.toShort(key))
      );
    }
    if (type == DataType.BINARY){
      return (T) bytes;
    } else if (type == DataType.TEXT) {
      String value = Bytes.toString(bytes);
      return (T) value;
    } else if (type == DataType.RECORDS){
      String value = Bytes.toString(bytes);
      List<Record> records = gson.fromJson(value, new TypeToken<List<Record>>(){}.getType());
      return (T) records;
    } else {
      throw new WorkspaceException("Unknown retrieval type");
    }
  }

  /**
   * Returns the type of content stored within the workspace.
   *
   * Workspace can store types of data as defined in {@link DataType} class.
   *
   * @param workspace name of the workspace for which the stored content type is stored.
   * @return string representation of the type defined in {@link DataType} if found, null otherwise.
   * @see DataType
   */
  @ReadOnly
  public DataType getType(String workspace)  {
    byte[] bytes = table.get(Bytes.toBytes(workspace), TYPE_COL);
    DataType type = DataType.fromString(Bytes.toString(bytes));
    return type;
  }

  private byte[] toKey(String value) {
    return Bytes.toBytes(value);
  }

  private byte[] toJsonBytes(Map<String, String> properties) {
    String value = gson.toJson(properties);
    return Bytes.toBytes(value);
  }

  private Map<String, String> fromJsonBytes(byte[] bytes) {
    String value = Bytes.toString(bytes);
    return gson.fromJson(value, Map.class);
  }

}
