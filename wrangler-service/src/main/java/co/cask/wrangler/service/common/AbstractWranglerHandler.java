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

package co.cask.wrangler.service.common;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.dataset.connections.ConnectionNotFoundException;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.Workspace;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceNotFoundException;
import co.cask.wrangler.proto.Recipe;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionType;
import org.apache.tephra.TransactionFailureException;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import static co.cask.wrangler.ServiceUtils.error;

/**
 * Common functionality for wrangler services.
 */
public class AbstractWranglerHandler extends AbstractHttpServiceHandler {
  protected ConnectionStore store;

  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
  private Table connectionTable;

  @UseDataSet(WorkspaceDataset.DATASET_NAME)
  private Table workspaceTable;

  protected WorkspaceDataset ws;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(connectionTable);
    ws = new WorkspaceDataset(workspaceTable);
  }

  /**
   * Return whether the header needs to be copied when creating the pipeline source for the specified workspace.
   * This just amounts to checking whether parse-as-csv with the first line as a header is used as a directive.
   */
  protected boolean shouldCopyHeader(@Nullable String workspaceId) {
    if (workspaceId == null) {
      return false;
    }
    try {
      Workspace workspace = ws.getWorkspace(workspaceId);
      Request request = workspace.getRequest();
      if (request == null) {
        return false;
      }
      Recipe recipe = request.getRecipe();
      List<String> directives = recipe.getDirectives();
      // yes this is really hacky, but there doesn't seem to be a good way to get the actual directive classes
      return directives.stream()
        .map(String::trim)
        .anyMatch(directive -> directive.startsWith("parse-as-csv") && directive.endsWith("true"));
    } catch (WorkspaceNotFoundException e) {
      return false;
    }
  }

  /**
   * Validates that the specified connection exists and is of the expected type.
   * Returns null if the connection does not exist or is invalid. Callers should return immediately if
   * a null is returned, as a response has already been sent. This method should only be called from endpoints
   * that use explicit transaction control.
   *
   * @param connectionId the id of the connection
   * @param expectedType the expected type of the connection
   * @param responder http responder
   * @return the validated connection, or null if it does not exist or is invalid
   */
  @Nullable
  protected Connection getValidatedConnection(String connectionId, ConnectionType expectedType,
                                              HttpServiceResponder responder) {
    AtomicReference<Connection> connectionRef = new AtomicReference<>();
    try {
      getContext().execute(datasetContext -> connectionRef.set(store.get(connectionId)));
    } catch (TransactionFailureException e) {
      if (e.getCause() instanceof ConnectionNotFoundException) {
        ServiceUtils.notFound(responder, e.getCause().getMessage());
        return null;
      }
      ServiceUtils.error(responder, e.getMessage());
      return null;
    }
    Connection connection = connectionRef.get();
    if (expectedType != connection.getType()) {
      error(responder, "Invalid connection type set, this endpoint only accepts GCS connection type");
      return null;
    }
    return connection;
  }
}
