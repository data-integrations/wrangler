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

package co.cask.wrangler.service.recipe;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveNotFoundException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.recipe.RecipeDatum;
import co.cask.wrangler.proto.recipe.RecipeInfo;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import co.cask.wrangler.registry.UserDirectiveRegistry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
  private static final byte[] ID = Bytes.toBytes("id");
  private static final byte[] NAME = Bytes.toBytes("name");
  private static final byte[] CREATED = Bytes.toBytes("created");
  private static final byte[] UPDATED = Bytes.toBytes("updated");
  private static final byte[] DESCRIPTION = Bytes.toBytes("description");
  private static final byte[] DIRECTIVES = Bytes.toBytes("directives");

  @UseDataSet(DATASET)
  private Table recipeStore;

  private final Gson gson = new Gson();

  /**
   * Creates a recipe entry in the recipe store.
   *
   * Following is the response
   * {
   *   "status" : 200,
   *   "message" : "Successfully created recipe with id 'test'"
   * }
   *
   * TODO: (CDAP-14652) correct the REST semantics, this should be a POST
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param description (Query Argument) for the recipe to be stored.
   * @param name (Query Argument) display name of the recipe.
   */
  @PUT
  @Path("recipes")
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @QueryParam("description") String description,
                     @QueryParam("name") String name) {
    String recipeId = getIdFromName(name);
    try {
      RecipeDatum datum = read(recipeId);
      if (datum != null) {
        error(responder, String.format("Recipe with name '%s' (id: '%s') already exists. Use POST to update it.",
                                       name, recipeId));
        return;
      }

      List<String> directives = new ArrayList<>();
      ByteBuffer content = request.getContent();
      if (content != null && content.hasRemaining()) {
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        directives = gson.fromJson(Bytes.toString(content), new TypeToken<List<String>>() { }.getType());
      }

      long time = System.currentTimeMillis() / 1000;
      datum = new RecipeDatum(recipeId, name, description, time, time, directives);

      write(recipeId, datum);

      ServiceResponse<RecipeDatum> response = new ServiceResponse<>(datum);
      responder.sendJson(response);
    } catch (Exception e) {
      LOG.info(String.format("Error creating recipe '%s'.", recipeId), e);
      error(responder, e.getMessage());
    }
  }

  private String getIdFromName(String name) {
    name = name.toLowerCase();
    name = name.replaceAll("[_ \t]+", "-");
    name = name.replaceAll("[/$%#@**&()!,~+=?><|}{]+", "");
    return name;
  }

  private RecipeDatum read(String key) {
    Row row = recipeStore.get(Bytes.toBytes(key));
    if (row != null && !row.isEmpty()) {
      return read(row);
    }
    return null;
  }

  private RecipeDatum read(Row row) {
    String id = row.getString(ID);
    String name = row.getString(NAME);
    String description = row.getString(DESCRIPTION);
    long created = row.getLong(CREATED);
    long updated = row.getLong(UPDATED);
    String recipe = row.getString(DIRECTIVES);
    List<String> directives = gson.fromJson(recipe, new TypeToken<List<String>>() { }.getType());
    RecipeDatum datum = new RecipeDatum(id, name, description, created, updated, directives);
    return datum;
  }

  private void write(String key, RecipeDatum datum) {
    byte[][] columns = new byte[][] {
      ID, NAME, DESCRIPTION, CREATED, UPDATED, DIRECTIVES
    };

    String directivesJson = gson.toJson(datum.getDirectives());

    byte[][] values = new byte[][] {
      Bytes.toBytes(key),
      Bytes.toBytes(datum.getName()),
      Bytes.toBytes(datum.getDescription()),
      Bytes.toBytes(datum.getCreated()),
      Bytes.toBytes(datum.getUpdated()),
      Bytes.toBytes(directivesJson)
    };
    recipeStore.put(Bytes.toBytes(key), columns, values);
  }

  /**
   * Lists all the recipes.
   * Following is a response returned
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("recipes/list")
  public void list(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      try (Scanner scanner = recipeStore.scan(null, null)) {
        List<RecipeDatum> values = new ArrayList<>();
        Row next;
        while ((next = scanner.next()) != null) {
          RecipeDatum recipeDatum = read(next);
          values.add(recipeDatum);
        }
        ServiceResponse<RecipeDatum> response = new ServiceResponse<>(values);
        responder.sendJson(response);
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
  @Path("recipes/{recipeId}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("recipeId") String recipeId) {
    try {
      RecipeDatum object = read(recipeId);
      if (object == null) {
        notFound(responder, String.format("Recipe with id '%s' not found", recipeId));
      } else {
        RecipeInfo recipeInfo = new RecipeInfo(object.getId(), object.getName(), object.getDescription(),
                                               object.getCreated(), object.getUpdated(), object.getDirectives());
        ServiceResponse<RecipeInfo> response = new ServiceResponse<>(recipeInfo);
        responder.sendJson(response);
      }
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

    RecipeDatum datum = read(recipeId);
    if (datum == null) {
      notFound(responder, String.format("Recipe id '%s' not found.", recipeId));
      return;
    }

    try {
      ByteBuffer content = request.getContent();
      if (content != null && content.hasRemaining()) {
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        List<String> directives = gson.fromJson(Bytes.toString(content), new TypeToken<List<String>>() { }.getType());

        CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
          new SystemDirectiveRegistry(),
          new UserDirectiveRegistry(getContext())
        );

        String migrate = new MigrateToV2(directives).migrate();
        RecipeParser parser = new GrammarBasedParser(migrate, registry);
        parser.initialize(null);
        parser.parse();

        RecipeDatum updated = new RecipeDatum(datum.getId(), datum.getName(), datum.getDescription(),
                                              datum.getCreated(), System.currentTimeMillis() / 1000, directives);
        write(recipeId, updated);
        success(responder, "Successfully updated directives for recipe id '" + recipeId + "'.");
      } else {
        error(responder, String.format("No valid directives present to be updated for recipe '%s'", recipeId));
      }
    } catch (DirectiveLoadException | DirectiveNotFoundException | DirectiveParseException e) {
      error(responder, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
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
