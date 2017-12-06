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

package co.cask.wrangler.service.connections;

import co.cask.cdap.api.Predicate;
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
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.net.HttpURLConnection;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.notFound;
import static co.cask.wrangler.ServiceUtils.sendJson;

/**
 * This service exposes REST APIs for managing the lifecycle of a connection in the connection store.
 */
public class ConnectionService extends AbstractHttpServiceHandler {

  // Data Prep store which stores all the information associated with dataprep.
  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
  private Table table;

  // Abstraction over the table defined above for managing connections.
  private ConnectionStore store;

  /**
   * Stores the context so that it can be used later.
   *
   * @param context the HTTP service runtime context
   */
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(table);
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
      JsonObject response = new JsonObject();
      JsonArray values = new JsonArray();
      values.add(new JsonPrimitive(id));
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
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

      // Return the id in the response.
      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
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
                   @QueryParam("type") final String type) {
    try {
      List<Connection> connections = store.scan(new Predicate<Connection>() {
        @Override
        public boolean apply(@Nullable Connection input) {
          if (type == null) {
            return false;
          }
          if(type.equalsIgnoreCase("*") ||
            input.getType().name().equalsIgnoreCase(type)) {
            return true;
          }
          return false;
        }
      });

      // Return the id in the response.
      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      JsonArray values = new JsonArray();
      for(Connection connection : connections) {
        JsonObject object = new JsonObject();
        object.addProperty("id", connection.getId());
        object.addProperty("name", connection.getName());
        object.addProperty("description", connection.getDescription());
        object.addProperty("created", connection.getCreated());
        object.addProperty("updated", connection.getUpdated());
        object.addProperty("type", connection.getType().name());
        values.add(object);
      }
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
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
      store.delete(id);

      // Return the id in the response.
      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
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

      // Return the id in the response.
      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty("id", connection.getId());
      object.addProperty("name", connection.getName());
      object.addProperty("description", connection.getDescription());
      object.addProperty("created", connection.getCreated());
      object.addProperty("updated", connection.getUpdated());
      object.addProperty("type", connection.getType().name());
      JsonObject properties = (JsonObject) new Gson().toJsonTree(connection.getAllProps());
      object.add("properties", properties);
      values.add(object);
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
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

      // Return the id in the response.
      JsonObject response = new JsonObject();
      JsonArray values = new JsonArray();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      JsonObject properties = (JsonObject) new Gson().toJsonTree(connection.getAllProps());
      values.add(properties);
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
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

      // Return the id in the response.
      JsonObject response = new JsonObject();
      JsonArray values = new JsonArray();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      JsonObject properties = (JsonObject) new Gson().toJsonTree(connection.getAllProps());
      values.add(properties);
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
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

      // Return the id in the response.
      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty("id", connection.getId());
      object.addProperty("name", connection.getName());
      object.addProperty("description", connection.getDescription());
      object.addProperty("created", connection.getCreated());
      object.addProperty("updated", connection.getUpdated());
      object.addProperty("type", connection.getType().name());
      JsonObject properties = (JsonObject) new Gson().toJsonTree(connection.getAllProps());
      object.add("properties", properties);
      values.add(object);
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }
}
