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

import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.service.http.SystemHttpServiceContext;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.UnauthorizedException;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionMeta;
import co.cask.wrangler.proto.connection.ConnectionType;
import co.cask.wrangler.service.common.AbstractWranglerHandler;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

/**
 * This service exposes REST APIs for managing the lifecycle of a connection in the connection store.
 */
public class ConnectionHandler extends AbstractWranglerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionHandler.class);
  private static final String CONNECTION_TYPE_CONFIG = "connectionTypeConfig";
  private static final Gson GSON = new Gson();
  private static final String ALL_TYPES = "*";

  private ConnectionTypeConfig connectionTypeConfig;
  private List<ConnectionTypeInfo> enabledConnectionTypes;

  public ConnectionHandler(ConnectionTypeConfig connectionTypeConfig) {
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
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    // initialize available connections
    this.enabledConnectionTypes = new ArrayList<>();
    String connectionTypeConfigString = context.getSpecification().getProperty(CONNECTION_TYPE_CONFIG);
    connectionTypeConfig = GSON.fromJson(connectionTypeConfigString, ConnectionTypeConfig.class);
    Set<ConnectionType> disabled = connectionTypeConfig.getDisabledTypes();
    for (ConnectionType connectionType : ConnectionType.values()) {
      if (!disabled.contains(connectionType)) {
        enabledConnectionTypes.add(new ConnectionTypeInfo(connectionType));
      }
    }
    validateAndCreateDefaultConnections();
  }

  private void validateAndCreateDefaultConnections() throws IOException {
    List<Connection> defaultConnections = connectionTypeConfig.getConnections();
    for (Connection defaultConnection : defaultConnections) {
      if (defaultConnection.getName() == null) {
        LOG.warn("Skipping the default connection without name field");
        continue;
      }
      if (defaultConnection.getType() == null) {
        LOG.warn("Skipping the default connection {}, type is missing or un-recognized", defaultConnection.getName());
        continue;
      }
      String namespace = defaultConnection.getNamespace();
      if (!getContext().getAdmin().namespaceExists(namespace)) {
        LOG.warn("Skipping connection '{}' since namespace '{}' does not exist",
                 defaultConnection.getName(), namespace);
        continue;
      }
      try {
        TransactionRunners.run(getContext(), context -> {
          ConnectionStore store = ConnectionStore.get(context);
          if (!store.connectionExists(namespace, defaultConnection.getName())) {
            store.create(namespace, defaultConnection);
          }
        });
      } catch (Exception e) {
        // we don't want to disrupt data-prep if we are not able to create a default connection,
        // log the error and continue
        LOG.warn("Exception while creating default connection ", e);
      }
    }
  }

  @POST
  @Path("connections/create")
  public void create(HttpServiceRequest request, HttpServiceResponder responder) {
    create(request, responder, getContext().getNamespace());
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
  @Path("contexts/{context}/connections/create")
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace) {
    respond(request, responder, namespace, () -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta();

      // Create an instance of the connection, if the connection id already exists,
      // it will throw an exception.
      NamespacedId id = TransactionRunners.run(getContext(), context -> {
        ConnectionStore store = ConnectionStore.get(context);
        return store.create(namespace, connection);
      });
      // Return the id in the response.
      return new ServiceResponse<>(Collections.singletonList(id.getId()));
    });
  }

  @POST
  @Path("connections/{id}/update")
  public void update(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    update(request, responder, getContext().getNamespace(), id);
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
  @Path("contexts/{context}/connections/{id}/update")
  public void update(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta();

      // Create an instance of the connection, if the connection id doesn't exist
      // it will throw an exception.
      TransactionRunners.run(getContext(), context -> {
        ConnectionStore store = ConnectionStore.get(context);
        store.update(new NamespacedId(namespace, id), connection);
      });
      return new ServiceResponse<>(Collections.emptyList());
    });
  }

  @GET
  @Path("connections")
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @DefaultValue(ALL_TYPES) @QueryParam("type") String type) {
    list(request, responder, getContext().getNamespace(), type);
  }

  /**
   * Lists all the connections available in the connection store.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @GET
  @Path("contexts/{context}/connections")
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("context") String namespace, @DefaultValue(ALL_TYPES) @QueryParam("type") String type) {
    respond(request, responder, namespace, () -> {
      List<Connection> connections = TransactionRunners.run(getContext(), context -> {
        ConnectionStore store = ConnectionStore.get(context);
        return store.list(namespace, input -> {
          if (input == null) {
            return false;
          }
          if (connectionTypeConfig.getDisabledTypes().contains(input.getType())) {
            return false;
          }
          return type.equalsIgnoreCase(ALL_TYPES) || input.getType().name().equalsIgnoreCase(type);
        });
      });
      return new ConnectionResponse<>(connections,
                                      getDefaultConnection(namespace, connections, connectionTypeConfig));
    });
  }

  @Nullable
  private String getDefaultConnection(String namespace, List<Connection> connections,
                                      ConnectionTypeConfig connectionTypeConfig) {
    NamespacedId defaultConnection = connectionTypeConfig.getDefaultConnection();
    if (defaultConnection == null) {
      return null;
    }
    if (!namespace.equals(defaultConnection.getNamespace())) {
      return null;
    }
    Optional<Connection> connection =
      connections.stream().filter(e -> defaultConnection.equals(e.getId())).findFirst();
    return connection.map(Connection::getId).map(NamespacedId::getId).orElse(null);
  }

  @DELETE
  @Path("connections/{id}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    delete(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Deletes a connection from the connection store.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id of the connection to be deleted.
   */
  @DELETE
  @Path("contexts/{context}/connections/{id}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      List<Connection> preConfiguredConnections = connectionTypeConfig.getConnections();
      // pre-configured connections provided as part of config only contains name and type of connection
      // since connection id is derived from connection name, we get the connection-id for the
      // pre-configured connection name and check with the input connection id.
      if (preConfiguredConnections.stream().anyMatch(c -> id.equals(ConnectionStore.getConnectionId(c.getName())))) {
        throw new UnauthorizedException(String.format("Cannot delete admin controlled connection %s", id));
      }
      TransactionRunners.run(getContext(), context -> {
        ConnectionStore store = ConnectionStore.get(context);
        store.delete(new NamespacedId(namespace, id));
      });
      return new ServiceResponse<Connection>(new ArrayList<>());
    });
  }

  @GET
  @Path("connections/{id}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("id") String id) {
    get(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Given a connection id, returns the complete information about the connection.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id for which all the information is returned from the connection store.
   */
  @GET
  @Path("contexts/{context}/connections/{id}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> getConnection(new NamespacedId(namespace, id)));
  }


  @GET
  @Path("connections/{id}/properties")
  public void properties(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("id") String id) {
    properties(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Returns only the properties of the connection requested.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id of the connection for which all properties need to be returned.
   */
  @GET
  @Path("contexts/{context}/connections/{id}/properties")
  public void properties(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> getConnection(new NamespacedId(namespace, id)).getProperties());
  }

  @PUT
  @Path("connections/{id}/properties")
  public void updateProp(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("id") String id,
                         @QueryParam("key") String key, @QueryParam("value") String value) {
    updateProp(request, responder, getContext().getNamespace(), id, key, value);
  }

  /**
   * Updates a property of the connection.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id of the connection to be who's property needs to be updated.
   */
  @PUT
  @Path("contexts/{context}/connections/{id}/properties")
  public void updateProp(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace, @PathParam("id") String id,
                         @QueryParam("key") String key, @QueryParam("value") String value) {
    respond(request, responder, namespace, () -> {

      NamespacedId namespacedId = new NamespacedId(namespace, id);
      return TransactionRunners.run(getContext(), context -> {
        ConnectionStore store = ConnectionStore.get(context);
        Connection connection = store.get(namespacedId);

        ConnectionMeta updatedMeta = ConnectionMeta.builder(connection)
          .putProperty(key, value)
          .build();
        store.update(namespacedId, updatedMeta);
        return new ServiceResponse<>(updatedMeta.getProperties());
      });
    });
  }

  @GET
  @Path("connections/{id}/clone")
  public void clone(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("id") final String id) {
    clone(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Given a connection id, Clones and returns a new connection.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id of the connection that should be cloned.
   */
  @GET
  @Path("contexts/{context}/connections/{id}/clone")
  public void clone(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      Connection connection = getConnection(new NamespacedId(namespace, id));
      ConnectionMeta clone = ConnectionMeta.builder(connection)
        .setName(connection.getName() + "_Clone")
        .build();
      return new ServiceResponse<>(clone);
    });
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
