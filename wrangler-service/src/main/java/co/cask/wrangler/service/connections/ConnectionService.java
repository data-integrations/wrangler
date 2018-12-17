/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.wrangler.service.connections;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.proto.ServiceResponse;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.notFound;

/**
 * This service exposes REST APIs for managing the lifecycle of a connection in the connection store.
 */
public class ConnectionService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionService.class);
  private static final String CONNECTION_TYPE_CONFIG = "connectionTypeConfig";
  private static final Gson GSON = new Gson();
  private static final String ALL_TYPES = "*";
  // Data Prep store which stores all the information associated with dataprep.
  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
  private Table table;

  // Abstraction over the table defined above for managing connections.
  private ConnectionStore store;
  private ConnectionTypeConfig connectionTypeConfig;
  private List<ConnectionTypeInfo> enabledConnectionTypes;

  public ConnectionService(ConnectionTypeConfig connectionTypeConfig) {
    this.connectionTypeConfig = connectionTypeConfig;
  }

  public void configure() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CONNECTION_TYPE_CONFIG, GSON.toJson(connectionTypeConfig));
    setProperties(properties);
  }

  /**
   * Stores the context so that it can be used later.
   *
   * @param context the HTTP service runtime context
   */
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(table);
    // initialize available connections
    this.enabledConnectionTypes = new ArrayList<>();
    String connectionTypeConfigString = context.getSpecification().getProperty(CONNECTION_TYPE_CONFIG);
    connectionTypeConfig = GSON.fromJson(connectionTypeConfigString, ConnectionTypeConfig.class);
    Set<ConnectionType> disabled = connectionTypeConfig.getDisabledTypes();
    for (ConnectionType connectionType : ConnectionType.values()) {
      if (!disabled.contains(connectionType) && !connectionType.equals(ConnectionType.UNDEFINED)) {
        enabledConnectionTypes.add(new ConnectionTypeInfo(connectionType));
      }
    }
    validateAndCreateDefaultConnections();
  }

  private void validateAndCreateDefaultConnections() {
    List<Connection> defaultConnections = connectionTypeConfig.getConnections();
    for (Connection defaultConnection : defaultConnections) {
      if (defaultConnection.getName() == null) {
        LOG.warn("Skipping the default connection without name field");
        continue;
      }
      if (defaultConnection.getType() == null || defaultConnection.getType() == ConnectionType.UNDEFINED) {
        LOG.warn("Skipping the default connection {}, type is missing or un-recognized", defaultConnection.getName());
        continue;
      }
      try {
        if (!store.connectionExists(defaultConnection.getName())) {
          store.create(defaultConnection);
        }
      } catch (Exception e) {
        // we don't want to disrupt data-prep if we are not able to create a default connection,
        // log the error and continue
        LOG.warn("Exception while creating default connection ", e);
      }
    }
  }

  /**
   * Creates a connection object in the connections store.
   *
   * Following is the JSON request for setting the connection.
   *
   * {
   *   "name" : "MySQL Production DB",
   *   "description" : "Production DB connection handler",
   *   "type" : "DATABASE",
   *   "properties" : {
   *     "hostname" : "localhost",
   *     "port" : 3306,
   *     "username" : "root",
   *     "password" : "",
   *     "ssl" : false
   *   }
   * }
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @POST
  @Path("connections/create")
  public void create(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent("utf-8", Connection.class);

      if (ConnectionType.fromString(connection.getType().getType()) == ConnectionType.UNDEFINED) {
        error(responder, "Invalid connection type set.");
        return;
      }

      // Create an instance of the connection, if the connection id already exists,
      // it will throw an exception.
      String id = store.create(connection);

      // Return the id in the response.
      responder.sendJson(new ServiceResponse<>(ImmutableList.of(id)));
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Updates the connection within the connections store.
   *
   * Following is the JSON request for setting the connection.
   *
   * {
   *   "id" : "mysql_production_db",
   *   "name" : "MySQL Production DB",
   *   "description" : "Production DB connection handler",
   *   "created" : 123323213,
   *   "updated" : 123453533,
   *   "type" : "DATABASE",
   *   "properties" : {
   *     "hostname" : "localhost",
   *     "port" : 3306,
   *     "username" : "root",
   *     "password" : "",
   *     "ssl" : false
   *   }
   * }
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @POST
  @Path("connections/{id}/update")
  public void update(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent("utf-8", Connection.class);

      if (ConnectionType.fromString(connection.getType().getType()) == ConnectionType.UNDEFINED) {
        error(responder, "Invalid connection type set.");
        return;
      }

      // Create an instance of the connection, if the connection id doesn't exist
      // it will throw an exception.
      store.update(id, connection);
      responder.sendJson(new ServiceResponse<>(ImmutableList.of()));
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Lists all the connections available in the connection store.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @GET
  @Path("connections")
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @DefaultValue (ALL_TYPES) @QueryParam("type") final String type) {
    try {
      List<Connection> connections = store.scan(input -> {
        if (input == null) {
          return false;
        }
        if (connectionTypeConfig.getDisabledTypes().contains(input.getType())) {
          return false;
        }
        if (type.equalsIgnoreCase(ALL_TYPES) || input.getType().name().equalsIgnoreCase(type)) {
          return true;
        }
        return false;
      });
      responder.sendJson(new ConnectionResponse<>(connections,
                                                  getDefaultConnection(connections, connectionTypeConfig)));
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  @Nullable
  private String getDefaultConnection(List<Connection> connections, ConnectionTypeConfig connectionTypeConfig) {
    String defaultConnection = connectionTypeConfig.getDefaultConnection();
    if (defaultConnection == null) {
      return null;
    }
    Optional<Connection> connection =
      connections.stream().filter(e -> defaultConnection.equals(e.getId())).findFirst();
    return connection.map(Connection::getId).orElse(null);
  }

  /**
   * Deletes a connection from the connection store.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id of the connection to be deleted.
   */
  @DELETE
  @Path("connections/{id}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") final String id) {
    try {
      List<Connection> preConfiguredConnections = connectionTypeConfig.getConnections();
      // pre-configured connections provided as part of config only contains name and type of connection
      // since connection id is derived from connection name, we get the connection-id for the
      // pre-configured connection name and check with the input connection id.
      if (preConfiguredConnections.stream()
        .filter(c -> id.equals(store.getConnectionId(c.getName()))).findFirst().isPresent()) {
        responder.sendError(HttpURLConnection.HTTP_UNAUTHORIZED,
                            String.format("Cannot delete admin controlled connection %s", id));
        return;
      }
      store.delete(id);
      responder.sendJson(new ServiceResponse<Connection>(new ArrayList<>()));
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Given a connection id, returns the complete information about the connection.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id for which all the information is returned from the connection store.
   */
  @GET
  @Path("connections/{id}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("id") final String id) {
    try {
      Connection connection = store.get(id);
      if (connection == null) {
        notFound(responder, String.format(
          "Connection with id '%s' not found in the store", id
        ));
        return;
      }
      responder.sendJson(new ServiceResponse<>(ImmutableList.of(connection)));
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Returns only the properties of the connection requested.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id of the connection for which all properties need to be returned.
   */
  @GET
  @Path("connections/{id}/properties")
  public void properties(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("id") final String id) {
    try {
      Connection connection = store.get(id);
      if (connection == null) {
        notFound(responder, String.format(
          "Connection with id '%s' not found in the store", id
        ));
        return;
      }
      responder.sendJson(new ServiceResponse<>(ImmutableList.of(connection.getAllProps())));
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Updates a property of the connection.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id of the connection to be who's property needs to be updated.
   */
  @PUT
  @Path("connections/{id}/properties")
  public void updateProp(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("id") final String id, @QueryParam("key") String key,
                         @QueryParam("value") String value) {
    try {
      Connection connection = store.get(id);
      if (connection == null) {
        notFound(responder, String.format(
          "Connection with id '%s' not found in the store", id
        ));
        return;
      }

      connection.putProp(key, value);
      store.update(id, connection);
      responder.sendJson(new ServiceResponse<>(ImmutableList.of(connection.getAllProps())));
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Given a connection id, Clones and returns a new connection.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id of the connection that should be cloned.
   */
  @GET
  @Path("connections/{id}/clone")
  public void clone(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("id") final String id) {
    try {
      Connection connection = store.clone(id);
      if (connection == null) {
        notFound(responder, String.format(
          "Connection with id '%s' not found in the store", id
        ));
        return;
      }
      responder.sendJson(new ServiceResponse<>(ImmutableList.of(connection)));
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Get the list of connection types that are supported
   */
  @GET
  @Path("connectionTypes")
  public void getConnectionTypes(HttpServiceRequest request, HttpServiceResponder responder) {
    responder.sendJson(enabledConnectionTypes);
  }
}
