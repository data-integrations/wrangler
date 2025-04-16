/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler.store.workspace;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.dataset.workspace.WorkspaceNotFoundException;
import io.cdap.wrangler.proto.workspace.v2.Workspace;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceDetail;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceId;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Workspace store for v2 endpoint
 */
public class WorkspaceStore {
  private static final StructuredTableId TABLE_ID = new StructuredTableId("workspaces_store");
  private static final String NAMESPACE_FIELD = "namespace";
  private static final String WORKSPACE_ID_FIELD = "workspace_id";
  // this field is to ensure the workspace information is correctly fetched if a namespace is recreated
  private static final String GENERATION_COL = "generation";
  private static final String CREATED_COL = "createdtimemillis";
  private static final String UPDATED_COL = "updatedtimemillis";
  private static final String SAMPLE_COL = "sample";
  private static final String WORKSPACE_INFO_COL = "workspace_info";

  public static final StructuredTableSpecification WORKSPACE_TABLE_SPEC =
    new StructuredTableSpecification.Builder()
      .withId(TABLE_ID)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.longType(GENERATION_COL),
                  Fields.stringType(WORKSPACE_ID_FIELD),
                  Fields.longType(CREATED_COL),
                  Fields.longType(UPDATED_COL),
                  Fields.bytesType(SAMPLE_COL),
                  Fields.stringType(WORKSPACE_INFO_COL))
      .withPrimaryKeys(NAMESPACE_FIELD, GENERATION_COL, WORKSPACE_ID_FIELD)
      .build();

  private static final Gson GSON = new GsonBuilder()
                                     .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private final TransactionRunner transactionRunner;

  public WorkspaceStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Get the workspace about the given workspace id
   *
   * @param workspaceId the id of the workspace to look up
   * @return the workspace metadata about the given workspace id
   * @throws WorkspaceNotFoundException if the workspace is not found
   */
  public Workspace getWorkspace(WorkspaceId workspaceId) throws WorkspaceNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      return getWorkspaceInternal(table, workspaceId, true);
    }, WorkspaceNotFoundException.class);
  }

  /**
   * Get the workspace detail about the given workspace id
   *
   * @param workspaceId the id of the workspace to look up
   * @return the workspace detail about the given workspace id
   * @throws WorkspaceNotFoundException if the workspace is not found
   */
  public WorkspaceDetail getWorkspaceDetail(WorkspaceId workspaceId) throws WorkspaceNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Optional<StructuredRow> row = table.read(getWorkspaceKeys(workspaceId));
      if (!row.isPresent()) {
        throw new WorkspaceNotFoundException(
          String.format("Workspace %s does not exist", workspaceId.getWorkspaceId()));
      }

      Workspace workspace = GSON.fromJson(row.get().getString(WORKSPACE_INFO_COL), Workspace.class);
      List<Row> rows = new ArrayList<>();
      byte[] sample = row.get().getBytes(SAMPLE_COL);
      if (sample != null) {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(sample))) {
          rows = (List<Row>) ois.readObject();
        }
      }

      return new WorkspaceDetail(workspace, rows);
    }, WorkspaceNotFoundException.class);
  }

  /**
   * Get all the workspaces in the given namespace
   *
   * @param namespace the namespace to look up
   * @return the list of workspaces in this namespace
   */
  public List<Workspace> listWorkspaces(NamespaceSummary namespace) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Range range = Range.singleton(getNamespaceKeys(namespace));
      List<Workspace> workspaces = new ArrayList<>();
      try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
        rowIter.forEachRemaining(
          structuredRow -> workspaces.add(GSON.fromJson(structuredRow.getString(WORKSPACE_INFO_COL),
                                                        Workspace.class)));
      }
      return workspaces;
    });
  }

  /**
   * Create/update the workspace from given workspace.
   *
   * @param workspaceId the id of the workspace
   * @param workspace workspace to create/update
   */
  public void saveWorkspace(WorkspaceId workspaceId, WorkspaceDetail workspace) {
    saveWorkspace(workspaceId, workspace.getWorkspace(), workspace.getSampleAsBytes(), false);
  }

  /**
   * Save the new workspace metadata
   *
   * @param workspaceId the workspace id
   * @param workspace the new workspace meta to save
   */
  public void updateWorkspace(WorkspaceId workspaceId, Workspace workspace) {
    saveWorkspace(workspaceId, workspace, null, true);
  }

  /**
   * Delete the given workspace
   *
   * @param workspaceId the workspace id to delete
   * @throws WorkspaceNotFoundException if the workspace is not found
   */
  public void deleteWorkspace(WorkspaceId workspaceId) throws WorkspaceNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      getWorkspaceInternal(table, workspaceId, true);
      table.delete(getWorkspaceKeys(workspaceId));
    }, WorkspaceNotFoundException.class);
  }

  // clean up all workspaces, only usable by tests, do not add @VisibleForTesting to not
  // introduce extra guava dependency
  void clear() {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      table.deleteAll(Range.all());
    });
  }

  private void saveWorkspace(WorkspaceId workspaceId, Workspace workspace, @Nullable byte[] sample,
                             boolean failIfNotFound) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      Workspace oldWorkspace  = getWorkspaceInternal(table, workspaceId, failIfNotFound);
      Workspace newWorkspace = workspace;

      if (oldWorkspace != null) {
        newWorkspace =
          Workspace.builder(newWorkspace).setCreatedTimeMillis(oldWorkspace.getCreatedTimeMillis()).build();
      }

      Collection<Field<?>> fields = getWorkspaceKeys(workspaceId);
      fields.add(Fields.longField(CREATED_COL, newWorkspace.getCreatedTimeMillis()));
      fields.add(Fields.longField(UPDATED_COL, newWorkspace.getUpdatedTimeMillis()));
      fields.add(Fields.stringField(WORKSPACE_INFO_COL, GSON.toJson(newWorkspace)));

      if (sample == null) {
        table.upsert(fields);
        return;
      }

      fields.add(Fields.bytesField(SAMPLE_COL, sample));
      table.upsert(fields);
    });
  }

  // internal get method so the save, delete and get operation can all happen in single transaction
  @Nullable
  private Workspace getWorkspaceInternal(
    StructuredTable table, WorkspaceId workspaceId,
    boolean failIfNotFound) throws IOException, WorkspaceNotFoundException {
    Optional<StructuredRow> row = table.read(getWorkspaceKeys(workspaceId));
    if (!row.isPresent()) {
      if (!failIfNotFound) {
        return null;
      }
      throw new WorkspaceNotFoundException(String.format("Workspace %s does not exist", workspaceId.getWorkspaceId()));
    }

    return GSON.fromJson(row.get().getString(WORKSPACE_INFO_COL), Workspace.class);
  }

  private Collection<Field<?>> getWorkspaceKeys(WorkspaceId workspace) {
    List<Field<?>> keys = new ArrayList<>(getNamespaceKeys(workspace.getNamespace()));
    keys.add(Fields.stringField(WORKSPACE_ID_FIELD, workspace.getWorkspaceId()));
    return keys;
  }

  private Collection<Field<?>> getNamespaceKeys(NamespaceSummary namespace) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(NAMESPACE_FIELD, namespace.getName()));
    keys.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    return keys;
  }
}
