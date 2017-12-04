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

package co.cask.wrangler.service.recipe;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.List;
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
import static co.cask.wrangler.ServiceUtils.success;

/**
 * This service handler handles management of recipes.
 *
 * <p>
 *   The recipe lifecycle involves
 *   <ul>
 *     <li>Creating recipe,</li>
 *     <li>Adding directives to recipe,</li>
 *     <li>Updating recipe and</li>
 *     <li>Deleting recipe</li>
 *   </ul>
 * </p>
 */
public class RecipeService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RecipeService.class);
  public static final String DATASET = "recipes";

  @UseDataSet(DATASET)
  private ObjectMappedTable<RecipeDatum> recipeStore;

  /**
   * Creates a recipe entry in the recipe store.
   *
   * Following is the response
   * {
   *   "status" : 200,
   *   "message" : "Successfully created recipe with id 'test'"
   * }
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param recipeId id of the recipe to be stored.
   * @param description (Query Argument) for the recipe to be stored.
   * @param name (Query Argument) display name of the recipe.
   */
  @PUT
  @Path("recipes/{recipeId}")
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("recipeId") String recipeId,
                     @QueryParam("description") String description,
                     @QueryParam("name") String name) {
    try {
      RecipeDatum datum = recipeStore.read(Bytes.toBytes(recipeId));
      if (datum == null) {
        recipeStore.write(Bytes.toBytes(recipeId), new RecipeDatum(recipeId, name, description));
      } else {
        datum.setUpdated(System.currentTimeMillis() / 1000);
        datum.setDescription(description);
        datum.setName(name);
        recipeStore.write(Bytes.toBytes(recipeId), datum);
      }
      success(responder, String.format("Successfully created recipe with id '%s'", recipeId));
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Lists all the recipes.
   * Following is a response returned
   *
   * {
   *   "status" : 200,
   *   "message" : "Success",
   *   "count" : 2,
   *   "values" : [
   *      "parse_titanic_csv",
   *      "ccda_hl7_xml"
   *   ]
   * }
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("recipes/list")
  public void list(HttpServiceRequest request, HttpServiceResponder responder) {
    JSONObject response = new JSONObject();
    Row row;
    try {
      try (CloseableIterator<KeyValue<byte[], RecipeDatum>> scanner = recipeStore.scan((byte[])null, (byte[]) null)) {
        JSONArray values = new JSONArray();
        while(scanner.hasNext()) {
          KeyValue<byte[], RecipeDatum> datum = scanner.next();
          values.put(new String(datum.getKey()));
        }
        response.put("status", HttpURLConnection.HTTP_OK);
        response.put("message", "Success");
        response.put("count", values.length());
        response.put("values", values);
        sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
      }
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Returns the information stored for a recipe.
   *
   * Following is the response
   *
   * {
   *    "status" : 200,
   *    "message" : "Success",
   *    "count" : 1,
   *    "values" : [
   *      {
   *        "id" : "test",
   *        "name" : "Test Executor",
   *        "description" : "Testing directive as example",
   *        "created" : 1490675972,
   *        "updated" : 1490675972,
   *        "length" : 2,
   *        "directives" : [
   *          "parse-as-csv body ,
   *          "drop body"
   *        ]
   *      }
   *    ]
   * }
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param recipeId id of the recipe requested.
   */
  @GET
  @Path("recipes/{recipeId}/versions/{version}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("recipeId") String recipeId) {
    JSONObject response = new JSONObject();
    try {
      RecipeDatum object = recipeStore.read(Bytes.toBytes(recipeId));
      if (object == null) {
        success(responder, String.format("TestRecipe with id '%s' not found", recipeId));
        return;
      } else {
        response.put("status", HttpURLConnection.HTTP_OK);
        response.put("message", "Success");
        response.put("count", 1);

        JSONArray values = new JSONArray();
        JSONObject o = new JSONObject();
        o.put("id", object.getId());
        o.put("name", object.getName());
        o.put("description", object.getDescription());
        o.put("created", object.getCreated());
        o.put("updated", object.getUpdated());
        JSONArray directives = new JSONArray();
        for (String directive : object.getDirectives()) {
          directives.put(directive);
        }
        o.put("length", object.getDirectives().size());
        o.put("directives", directives);
        values.put(o);
        response.put("values", values);
      }
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Updates a recipe with the directives.
   * The recipe should exist before updating directives.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param recipeId id of the recipe requested.
   */
  @POST
  @Path("recipes/{recipeId}")
  public void post(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("recipeId") String recipeId) {

    RecipeDatum datum = recipeStore.read(Bytes.toBytes(recipeId));
    if (datum == null) {
      notFound(responder, String.format("TestRecipe id '%s' not found.", recipeId));
      return;
    }

    ByteBuffer content = request.getContent();
    if (content != null && content.hasRemaining()) {
      GsonBuilder builder = new GsonBuilder();
      Gson gson = builder.create();
      List<String> directives = gson.fromJson(Bytes.toString(content), new TypeToken<List<String>>(){}.getType());
      datum.setDirectives(directives);
      datum.setUpdated(System.currentTimeMillis() / 1000);
      recipeStore.write(Bytes.toBytes(recipeId), datum);
      success(responder, "Successfully updated directives for recipe id '" + recipeId + "'.");
    } else {
      error(responder, String.format("No valid directives present to be updated for recipe '%s'", recipeId));
    }
  }

  /**
   * Deletes a recipe from the store.
   *
   * Following is the response
   * {
   *   "status" : 200,
   *   "message" : "Successfully deleted recipe with id 'test'"
   * }
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param recipeId to be deleted from the recipe store.
   */
  @DELETE
  @Path("recipes/{recipeId}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("recipeId") String recipeId) {
    try {
      recipeStore.delete(Bytes.toBytes(recipeId));
      success(responder, String.format("Successfully deleted recipe '%s'", recipeId));
    } catch (DataSetException e) {
      error(responder, "Unable to delete recipe with id '" + recipeId + "'. " + e.getMessage());
    } catch (Exception e) {
      error(responder, "Unable to delete recipe with id '" + recipeId + "'. " + e.getMessage());
    }
  }
}
