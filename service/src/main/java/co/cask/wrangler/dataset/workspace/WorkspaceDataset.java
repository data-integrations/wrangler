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
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.api.Record;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class WorkspaceDataset extends AbstractDataset {
  private static final Logger LOG = LoggerFactory.getLogger(WorkspaceDataset.class);
  private final Table table;
  private final Gson gson;
  public static final byte[] DATA_COL       = Bytes.toBytes("data");
  public static final byte[] NAME_COL       = Bytes.toBytes("name");
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
   * @param id id of workspace.
   * @param name of the workspace to display.
   * @throws WorkspaceException thrown when there is issue creating workspace.
   */
  @WriteOnly
  public void createWorkspaceMeta(String id, String name) throws WorkspaceException {
    createWorkspaceMeta(id, name, DataType.BINARY);
  }

  /**
   * Creates a workspace meta entry, with the type specified and empty properties.
   *
   * @param id id of the workspace to be created.
   * @param name of the workspace to display.
   * @param type of data in workspace.
   * @throws WorkspaceException thrown when issue creating workspace meta entry.
   */
  @WriteOnly
  public void createWorkspaceMeta(String id, String name, DataType type) throws WorkspaceException {
    createWorkspaceMeta(id, name, type, new HashMap<String, String>());
  }

  /**
   * Creates a workspace meta entry with the type specified and properties.
   *
   * @param id id of workspace to be created.
   * @param name name of workspace to be created.
   * @param type of data stored in workspace.
   * @param properties associated with workspace.
   * @throws WorkspaceException thrown when issue creating workspace meta entry.
   */
  @WriteOnly
  public void createWorkspaceMeta(String id, String name, DataType type,
                     Map<String, String> properties) throws WorkspaceException {
    if (id == null || id.isEmpty()) {
      throw new WorkspaceException("Workspace id cannot be empty or null");
    }

    byte[][] columns = new byte[][] {
     CREATED_COL, TYPE_COL, NAME_COL, PROPERTIES_COL
    };

    byte[][] data = new byte[][] {
      Bytes.toBytes(System.currentTimeMillis() / 1000),
      Bytes.toBytes(type.getType()),
      Bytes.toBytes(name),
      toJsonBytes(properties)
    };

    try {
      table.put(toKey(id), columns, data);
    } catch (DataSetException e) {
      throw new WorkspaceException(
        String.format("Unable to create workspace '%s'",
                      e.getMessage())
      );
    }
  }

  /**
   * Checks if a workspace exists.
   *
   * @param id of the workspace to be checked for.
   * @return true if workspace exists, false otherwise.
   * @throws WorkspaceException thrown if there are any issues with workspace.
   */
  public boolean hasWorkspace(String id) throws WorkspaceException {
    try {
      Row row = table.get(toKey(id));
      if (!row.isEmpty()) {
        return true;
      }
      return false;
    } catch (DataSetException e) {
      throw new WorkspaceException(
        String.format("Unable to check if workspace '%s' exists '%s'",
                      id, e.getMessage())
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
  public List<KeyValue<String, String>> getWorkspaces() throws WorkspaceException {
    List<KeyValue<String, String>> values = new ArrayList<>();
    Row row;
    try (Scanner scanner = table.scan(null, null)) {
      while((row = scanner.next()) != null) {
        byte[] key = row.getRow();
        byte[] name = row.get(NAME_COL);
        values.add(new KeyValue<>(Bytes.toString(key), Bytes.toString(name)));
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
   * @param id to be deleted.
   * @throws WorkspaceException thrown if there is issue deleting workspace.
   */
  @WriteOnly
  public void deleteWorkspace(String id) throws WorkspaceException {
    try {
      table.delete(toKey(id));
    } catch (DataSetException e){
      throw new WorkspaceException(
        String.format("Failed to delete workspace '%s'. %s", id, e.getMessage())
      );
    }
  }

  @WriteOnly
  public void writeToWorkspace(String id, byte[] key, DataType type, byte[] data)
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
      table.put(toKey(id), columns, bytes);
    } catch (DataSetException e) {
      throw new WorkspaceException(
        String.format("Unable to create workspace '%s'",
                      e.getMessage())
      );
    }
  }

  @WriteOnly
  public void updateWorkspace(String id, byte[] key, byte[] data)
    throws WorkspaceException {
    byte[][] columns = new byte[][] {
      UPDATED_COL, key
    };

    byte[][] bytes = new byte[][] {
      Bytes.toBytes(System.currentTimeMillis() / 1000),
      data
    };

    try {
      table.put(toKey(id), columns, bytes);
    } catch (DataSetException e) {
      throw new WorkspaceException(
        String.format("Unable to create workspace '%s'",
                      e.getMessage())
      );
    }
  }

  @WriteOnly
  public void updateWorkspace(String id, byte[] key, String data) throws WorkspaceException {
    updateWorkspace(id, key, data.getBytes(Charsets.UTF_8));
  }

  @WriteOnly
  public void writeProperties(String id, Map<String, String> properties) throws WorkspaceException {
    byte[] bytes = toJsonBytes(properties);
    updateWorkspace(id, PROPERTIES_COL, bytes);
  }

  @ReadWrite
  public void updateProperty(String id, String key, String value) throws WorkspaceException {
    byte[] bytes = getData(id, Bytes.toBytes(key));
    Map<String, String> properties = fromJsonBytes(bytes);
    properties.put(key, value);
    updateWorkspace(id, PROPERTIES_COL, toJsonBytes(properties));
  }

  @ReadOnly
  public Map<String, String> getProperties(String id) throws WorkspaceException {
    byte[] bytes = table.get(Bytes.toBytes(id), PROPERTIES_COL);
    return fromJsonBytes(bytes);
  }

  /**
   * Retrieves the data from the workspace provided the key.
   *
   * @param id id of the workspace.
   * @param key the key to be retrieved.
   * @return if key is found, returns the data, else returns null.
   */
  @ReadOnly
  public byte[] getData(String id, byte[] key) throws WorkspaceException {
    byte[] bytes = table.get(toKey(id), key);
    return bytes;
  }

  @ReadOnly
  public <T> T getData(String id, byte[] key, DataType type) throws WorkspaceException {
    byte[] bytes = table.get(toKey(id), key);
    if(bytes == null) {
      return null;
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
   * @param id id of the workspace for which the stored content type is stored.
   * @return string representation of the type defined in {@link DataType} if found, null otherwise.
   * @see DataType
   */
  @ReadOnly
  public DataType getType(String id)  {
    byte[] bytes = table.get(Bytes.toBytes(id), TYPE_COL);
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
