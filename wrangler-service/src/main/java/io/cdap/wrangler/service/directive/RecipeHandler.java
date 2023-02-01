/*
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
 */

package io.cdap.wrangler.service.directive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.wrangler.dataset.recipe.RecipePageRequest;
import io.cdap.wrangler.dataset.recipe.RecipeRow;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.recipe.v2.Recipe;
import io.cdap.wrangler.proto.recipe.v2.RecipeCreationRequest;
import io.cdap.wrangler.proto.recipe.v2.RecipeId;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.store.recipe.RecipeStore;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * v2 endpoints for recipe
 */
public class RecipeHandler extends AbstractWranglerHandler {
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private static final Pattern RECIPE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9 ]*");

  private RecipeStore recipeStore;

  // Injected by CDAP
  @SuppressWarnings("unused")
  private Metrics metrics;

  private static final String NUMBER_RECIPE_SAVED_METRIC = "recipe.saved";

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
      RecipeId recipeId = RecipeId.builder(ns).build();
      Recipe recipe = buildRecipeFromRequest(request, recipeId);
      recipeStore.createRecipe(recipeId, RecipeRow.builder(recipe).build());
      metrics.gauge(NUMBER_RECIPE_SAVED_METRIC, recipe.getDirectives().size());
      responder.sendJson(recipe);
    });
  }

  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/recipes/id/{recipe-id}")
  public void getRecipeById(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace,
                        @PathParam("recipe-id") String recipeId) {
    respond(responder, namespace, ns -> {
      RecipeId id = RecipeId.builder(ns).setRecipeId(recipeId).build();
      responder.sendJson(recipeStore.getRecipeById(id));
    });
  }

  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/recipes/name/{recipe-name}")
  public void getRecipeByName(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace,
                        @PathParam("recipe-name") String recipeName) {
    respond(responder, namespace, ns -> {
      responder.sendJson(recipeStore.getRecipeByName(ns, recipeName));
    });
  }

  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/recipes")
  public void listRecipes(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace,
                          @QueryParam("pageSize") Integer pageSize,
                          @QueryParam("pageToken") String pageToken,
                          @QueryParam("sortBy")String sortBy,
                          @QueryParam("sortOrder") String sortOrder) {
    respond(responder, namespace, ns -> {
      RecipePageRequest pageRequest = RecipePageRequest.builder(ns)
        .setPageSize(pageSize)
        .setPageToken(pageToken)
        .setSortBy(sortBy)
        .setSortOrder(sortOrder)
        .build();
      responder.sendJson(recipeStore.listRecipes(pageRequest));
    });
  }

  @DELETE
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/recipes/id/{recipe-id}")
  public void deleteRecipe(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace,
                           @PathParam("recipe-id") String recipeId) {
    respond(responder, namespace, ns-> {
      recipeStore.deleteRecipe(RecipeId.builder(ns).setRecipeId(recipeId).build());
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @PUT
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/recipes/id/{recipe-id}")
  public void updateRecipe(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace,
                           @PathParam("recipe-id") String recipeIdString) {
    respond(responder, namespace, ns-> {
      RecipeId recipeId = RecipeId.builder(ns).setRecipeId(recipeIdString).build();
      Recipe recipe = buildRecipeFromRequest(request, recipeId);
      recipeStore.updateRecipe(recipeId, RecipeRow.builder(recipe).build());
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  private void validateRecipeRequest(Recipe recipe) {
    if (recipe == null) {
      throw new BadRequestException("Request is empty");
    }
    if (recipe.getRecipeName() == null) {
      throw new BadRequestException("No name was specified for the Recipe");
    }
    if (recipe.getDescription() == null) {
      throw new BadRequestException("No description was specified for the Recipe");
    }
    if (recipe.getDirectives() == null || recipe.getDirectives().size() == 0) {
      throw new BadRequestException("No directives were specified for the Recipe");
    }
    if (!RECIPE_NAME_PATTERN.matcher(recipe.getRecipeName()).matches()) {
      throw new BadRequestException("Recipe name should contain only alphanumeric characters or spaces");
    }
  }

  private Recipe buildRecipeFromRequest(HttpServiceRequest request, RecipeId recipeId) {
    RecipeCreationRequest creationRequest = GSON.fromJson(
      StandardCharsets.UTF_8.decode(request.getContent()).toString(), RecipeCreationRequest.class);
    List<String> directives = creationRequest.getDirectives();
    long now = System.currentTimeMillis();

    Recipe recipe = Recipe.builder(recipeId)
      .setRecipeName(creationRequest.getRecipeName())
      .setDescription(creationRequest.getDescription())
      .setCreatedTimeMillis(now)
      .setUpdatedTimeMillis(now)
      .setDirectives(directives)
      .setRecipeStepsCount(directives.size())
      .build();
    validateRecipeRequest(recipe);
    return recipe;
  }
}
