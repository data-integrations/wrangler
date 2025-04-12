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

package io.cdap.wrangler.service.common;

import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.dataset.connections.ConnectionNotFoundException;
import io.cdap.wrangler.dataset.connections.ConnectionStore;
import io.cdap.wrangler.dataset.workspace.Workspace;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.dataset.workspace.WorkspaceNotFoundException;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.proto.ErrorRecordsException;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.Recipe;
import io.cdap.wrangler.proto.Request;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.StatusCodeException;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.workspace.v2.RecipeExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Common functionality for wrangler services.
 */
public class AbstractWranglerHandler extends AbstractSystemHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWranglerHandler.class);

  protected Workspace getWorkspace(NamespacedId workspaceId) {
    return TransactionRunners.run(getContext(), context -> {
      WorkspaceDataset ws = WorkspaceDataset.get(context);
      return ws.getWorkspace(workspaceId);
    });
  }

  /**
   * Return whether the header needs to be copied when creating the pipeline source for the specified workspace.
   * This just amounts to checking whether parse-as-csv with the first line as a header is used as a directive.
   */
  protected boolean shouldCopyHeader(WorkspaceDataset ws, @Nullable NamespacedId workspaceId) throws IOException {
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

  protected Connection getConnection(NamespacedId connectionId) {
    return TransactionRunners.run(getContext(), context -> {
      ConnectionStore store = ConnectionStore.get(context);
      return store.get(connectionId);
    });
  }

  protected Connection getValidatedConnection(NamespacedId connectionId, ConnectionType expectedType) {
    return TransactionRunners.run(getContext(), context -> {
      ConnectionStore store = ConnectionStore.get(context);
      return getValidatedConnection(store, connectionId, expectedType);
    });
  }

  /**
   * Validates that the specified connection exists and is of the expected type.
   * Returns null if the connection does not exist or is invalid. Callers should return immediately if
   * a null is returned, as a response has already been sent. This method should only be called from endpoints
   * that use explicit transaction control.
   *
   * @param store the connection store to read from
   * @param connectionId the id of the connection
   * @param expectedType the expected type of the connection
   * @return the validated connection
   * @throws ConnectionNotFoundException if the connection does not exist
   * @throws IOException if there was an error reading from the store
   */
  @Nullable
  protected Connection getValidatedConnection(ConnectionStore store, NamespacedId connectionId,
                                              ConnectionType expectedType) throws IOException {
    Connection connection = store.get(connectionId);
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
   * Utility method for executing an endpoint with common error handling built in.
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
   * @param callable the endpoint logic to run
   */
  protected <T> void respond(HttpServiceRequest request, HttpServiceResponder responder, Callable<T> callable) {

    try {
      T results = callable.call();
      responder.sendJson(results);
    } catch (StatusCodeException e) {
      responder.sendJson(e.getCode(), new ServiceResponse<>(e.getMessage()));
    } catch (ErrorRecordsException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST,
        new ServiceResponse<>(e.getErrorRecords(), false, e.getMessage()));
    } catch (JsonSyntaxException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST, new ServiceResponse<Void>(e.getMessage()));
    } catch (Throwable t) {
      LOG.warn("Error processing {} {}, resulting in a 500 response.", request.getMethod(), request.getRequestURI(), t);
      responder.sendJson(HttpURLConnection.HTTP_INTERNAL_ERROR, new ServiceResponse<Void>(t.getMessage()));
    }
  }

  /**
   * Utility method for executing an endpoint with common error handling and namespace checks built in.
   *
   * If the callable throws a {@link StatusCodeException}, the exception's status code and message will be used
   * to create the response.
   * If a {@link JsonSyntaxException} is thrown, a 400 response will be sent.
   * If anything else if thrown, a 500 response will be sent.
   * If nothing is thrown, the result of the callable will be sent as json.
   *
   * @param responder the http responder
   * @param namespace the namespace to check for
   * @param runnable the endpoint logic to run
   */
  protected void respond(HttpServiceResponder responder, String namespace,
                         NamespacedResponderRunnable runnable) {
    // system namespace does not officially exist, so don't check existence for system namespace.
    NamespaceSummary namespaceSummary;
    if (Contexts.SYSTEM.equals(namespace)) {
      namespaceSummary = new NamespaceSummary(Contexts.SYSTEM, "", 0L);
    } else {
      try {
        namespaceSummary = getContext().getAdmin().getNamespaceSummary(namespace);
        if (namespaceSummary == null) {
          responder.sendJson(HttpURLConnection.HTTP_NOT_FOUND,
                             new io.cdap.wrangler.proto.workspace.v2.ServiceResponse<>(
                               String.format("Namespace '%s' does not exist", namespace)));
          return;
        }
      } catch (IOException e) {
        responder.sendJson(HttpURLConnection.HTTP_INTERNAL_ERROR,
                           new io.cdap.wrangler.proto.workspace.v2.ServiceResponse<>(e.getMessage()));
        return;
      }
    }

    try {
      runnable.respond(namespaceSummary);
    } catch (RecipeException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST,
                         new RecipeExceptionResponse<>(e.getMessage(), e.getRowIndex(), e.getDirectiveIndex()));
    } catch (StatusCodeException e) {
      responder.sendJson(e.getCode(), new io.cdap.wrangler.proto.workspace.v2.ServiceResponse<>(e.getMessage()));
    } catch (ErrorRecordsException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST,
                         new io.cdap.wrangler.proto.workspace.v2.ServiceResponse<>(e.getErrorRecords(),
                                                                                   e.getMessage()));
    } catch (JsonSyntaxException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST,
                         new io.cdap.wrangler.proto.workspace.v2.ServiceResponse<>(e.getMessage()));
    } catch (RemoteExecutionException e) {
      responder.sendJson(getErrorCode(e.getCause().getRemoteExceptionClassName()),
                         new io.cdap.wrangler.proto.workspace.v2.ServiceResponse<>(e.getMessage()));
    } catch (Throwable t) {
      responder.sendJson(HttpURLConnection.HTTP_INTERNAL_ERROR,
                         new io.cdap.wrangler.proto.workspace.v2.ServiceResponse<>((t.getMessage())));
    }
  }

  private int getErrorCode(String exceptionName) {
    boolean isBadRequest = Stream.of(BadRequestException.class, ErrorRecordsException.class, JsonSyntaxException.class)
      .anyMatch(e -> e.getName().equals(exceptionName));
    return isBadRequest ? HttpURLConnection.HTTP_BAD_REQUEST : HttpURLConnection.HTTP_INTERNAL_ERROR;
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
   * @param namespace the namespace to check for
   * @param callable the endpoint logic to run
   */
  protected <T> void respond(HttpServiceRequest request, HttpServiceResponder responder, String namespace,
                             NamespacedResponder<T> callable) {
    // system namespace does not officially exist, so don't check existence for system namespace.
    NamespaceSummary namespaceSummary;
    if (Contexts.SYSTEM.equals(namespace)) {
      namespaceSummary = new NamespaceSummary(Contexts.SYSTEM, "", 0L);
    } else {
      try {
        namespaceSummary = getContext().getAdmin().getNamespaceSummary(namespace);
        if (namespaceSummary == null) {
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
      T results = callable.respond(new Namespace(namespaceSummary.getName(), namespaceSummary.getGeneration()));
      responder.sendJson(results);
    } catch (StatusCodeException e) {
      responder.sendJson(e.getCode(), new ServiceResponse<>(e.getMessage()));
    } catch (ErrorRecordsException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST,
          new ServiceResponse<>(e.getErrorRecords(), false, e.getMessage()));
    } catch (JsonSyntaxException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST, new ServiceResponse<Void>(e.getMessage()));
    } catch (Throwable t) {
      LOG.warn("Error processing {} {}, resulting in a 500 response.", request.getMethod(), request.getRequestURI(), t);
      responder.sendJson(HttpURLConnection.HTTP_INTERNAL_ERROR, new ServiceResponse<Void>(t.getMessage()));
    }
  }

  /**
   * Responds to a request within a namespace.
   *
   * @param <T> type of response object
   */
  protected interface NamespacedResponder<T> {
    T respond(Namespace namespace) throws Exception;
  }

  /**
   * Responds to a request within a namespace.
   */
  protected interface NamespacedResponderRunnable {
    void respond(NamespaceSummary namespace) throws Exception;
  }
}
