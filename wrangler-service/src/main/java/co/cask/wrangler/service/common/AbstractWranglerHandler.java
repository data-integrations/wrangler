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
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.dataset.connections.ConnectionNotFoundException;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.Workspace;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceNotFoundException;
import co.cask.wrangler.proto.BadRequestException;
import co.cask.wrangler.proto.Contexts;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.Recipe;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.StatusCodeException;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionType;
import com.google.gson.JsonSyntaxException;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Common functionality for wrangler services.
 */
public class AbstractWranglerHandler extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWranglerHandler.class);
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
  protected boolean shouldCopyHeader(@Nullable NamespacedId workspaceId) {
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
   * @return the validated connection, or null if it does not exist or is invalid
   */
  @Nullable
  protected Connection getValidatedConnection(NamespacedId connectionId, ConnectionType expectedType) throws Exception {
    AtomicReference<Connection> connectionRef = new AtomicReference<>();
    try {
      getContext().execute(datasetContext -> connectionRef.set(store.get(connectionId)));
    } catch (TransactionFailureException e) {
      if (e.getCause() instanceof ConnectionNotFoundException) {
        throw (ConnectionNotFoundException) e.getCause();
      }
      throw e;
    }
    Connection connection = connectionRef.get();
    if (connection.getType() == null) {
      throw new BadRequestException("Connection type must be specified.");
    }
    if (expectedType != connection.getType()) {
      throw new BadRequestException(String.format("Expected connection type '%s' but found '%s'.",
                                                  expectedType, connection.getType()));
    }
    return connection;
  }

  /**
   * Utility method for executing an endpoint with common error handling and namespace checks built in.
   * A response will always be sent after this method is called so the http responder should not be used after this.
   * The endpoint logic should also not use the responder in any way.
   *
   * If the callable throws a {@link StatusCodeException}, the exception's status code and message will be used
   * to create the response.
   * If a {@link JsonSyntaxException} is thrown, a 400 response will be sent.
   * If anything else if thrown, a 500 response will be sent.
   * If nothing is thrown, the result of the callable will be sent as json.
   *
   * @param request the http request
   * @param responder the http responder
   * @param namespace the namespace to check for, or null if no check needs to be performed
   * @param callable the endpoint logic to run
   */
  protected <T> void respond(HttpServiceRequest request, HttpServiceResponder responder, @Nullable String namespace,
                             Callable<T> callable) {
    // system namespace does not officially exist, so don't check existence for system namespace.
    if (namespace != null && !Contexts.SYSTEM.equals(namespace)) {
      try {
        if (!getContext().getAdmin().namespaceExists(namespace)) {
          responder.sendJson(HttpURLConnection.HTTP_NOT_FOUND,
                             new ServiceResponse<Void>(String.format("Namespace '%s' does not exist", namespace)));
          return;
        }
      } catch (IOException e) {
        responder.sendJson(HttpURLConnection.HTTP_INTERNAL_ERROR, new ServiceResponse<Void>(e.getMessage()));
        return;
      }
    }

    try {
      T results = callable.call();
      responder.sendJson(results);
    } catch (StatusCodeException e) {
      responder.sendJson(e.getCode(), new ServiceResponse<>(e.getMessage()));
    } catch (JsonSyntaxException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST, new ServiceResponse<Void>(e.getMessage()));
    } catch (Throwable t) {
      LOG.warn("Error processing {} {}, resulting in a 500 response.", request.getMethod(), request.getRequestURI(), t);
      responder.sendJson(HttpURLConnection.HTTP_INTERNAL_ERROR, new ServiceResponse<Void>(t.getMessage()));
    }
  }

  /**
   * The same as calling {@link #respond(HttpServiceRequest, HttpServiceResponder, String, Callable)}
   * with a null namespace.
   */
  protected <T> void respond(HttpServiceRequest request, HttpServiceResponder responder, Callable<T> callable) {
    respond(request, responder, null, callable);
  }
}
