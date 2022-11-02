/**
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.wrangler.service.directive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.wrangler.dataset.recipe.RecipeRow;
import io.cdap.wrangler.proto.recipe.v2.Recipe;
import io.cdap.wrangler.proto.recipe.v2.RecipeCreationRequest;
import io.cdap.wrangler.proto.recipe.v2.RecipeId;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.store.recipe.RecipeStore;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * v2 endpoints for recipe
 */
public class RecipeHandler extends AbstractWranglerHandler {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  private RecipeStore recipeStore;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    recipeStore = new RecipeStore(context);
  }

  @POST
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/recipes")
  public void createRecipe(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      RecipeId recipeId = new RecipeId(ns);
      long now = System.currentTimeMillis();

      RecipeCreationRequest creationRequest = GSON.fromJson(
        StandardCharsets.UTF_8.decode(request.getContent()).toString(), RecipeCreationRequest.class);

      List<String> directives = new ArrayList<>(creationRequest.getDirectives());
      Recipe recipe = Recipe.builder(recipeId.getRecipeId())
        .setRecipeName(creationRequest.getRecipeName())
        .setDescription(creationRequest.getDescription())
        .setCreatedTimeMillis(now)
        .setUpdatedTimeMillis(now)
        .setDirectives(directives)
        .setRecipeStepsCount(directives.size())
        .build();

      RecipeRow recipeRow = RecipeRow.builder(recipe).build();

      recipeStore.saveRecipe(recipeId, recipeRow);
      responder.sendJson(recipeId.getRecipeId());
    });
  }

  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/recipes/{id}")
  public void getRecipe(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace,
                        @PathParam("id") String recipeId) {
    respond(responder, namespace, ns -> {
      responder.sendString(GSON.toJson(recipeStore.getRecipe(new RecipeId(ns, recipeId))));
    });
  }

  // TODO: getRecipeByName API

  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/recipes")
  public void listRecipes(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace,
                          @QueryParam("sortBy")String sortBy,
                          @QueryParam("pageSize") Integer pageSize,
                          @QueryParam("pageToken") String pageToken) {
    if (pageToken == null) {
      throw new IllegalArgumentException("pageToken missing in request.");
    }

    respond(responder, namespace, ns -> {
      responder.sendString(GSON.toJson(recipeStore.listRecipes(ns, pageSize, pageToken, sortBy)));
    });
  }

  @DELETE
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/recipes/{id}")
  public void deleteRecipe(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace,
                           @PathParam("id") String recipeId) {
    respond(responder, namespace, ns-> {
      recipeStore.deleteRecipe(new RecipeId(ns, recipeId));
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }
}
