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

package io.cdap.wrangler.store.recipe;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.dataset.recipe.RecipeAlreadyExistsException;
import io.cdap.wrangler.dataset.recipe.RecipeNotFoundException;
import io.cdap.wrangler.dataset.recipe.RecipePageRequest;
import io.cdap.wrangler.dataset.recipe.RecipeRow;
import io.cdap.wrangler.proto.recipe.v2.Recipe;
import io.cdap.wrangler.proto.recipe.v2.RecipeId;
import io.cdap.wrangler.proto.recipe.v2.RecipeListResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static io.cdap.wrangler.store.utils.Stores.getNamespaceKeys;

/**
 * Recipe store for v2 endpoint
 */
public class RecipeStore {
  private static final StructuredTableId TABLE_ID = new StructuredTableId("recipes_store");
  public static final String NAMESPACE_FIELD = "namespace";
  public static final String GENERATION_COL = "generation";
  public static final String RECIPE_ID_FIELD = "recipe_id";
  public static final String RECIPE_NAME_FIELD = "recipe_name";
  public static final String CREATE_TIME_COL = "create_time";
  public static final String UPDATE_TIME_COL = "update_time";
  public static final String RECIPE_INFO_COL = "recipe_info";

  public static final StructuredTableSpecification RECIPE_TABLE_SPEC =
    new StructuredTableSpecification.Builder()
      .withId(TABLE_ID)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.longType(GENERATION_COL),
                  Fields.stringType(RECIPE_ID_FIELD),
                  Fields.stringType(RECIPE_NAME_FIELD),
                  Fields.longType(CREATE_TIME_COL),
                  Fields.longType(UPDATE_TIME_COL),
                  Fields.stringType(RECIPE_INFO_COL))
      .withPrimaryKeys(NAMESPACE_FIELD, GENERATION_COL, RECIPE_ID_FIELD)
      .withIndexes(RECIPE_NAME_FIELD, UPDATE_TIME_COL)
      .build();

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  private final TransactionRunner transactionRunner;

  public RecipeStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Create a new recipe
   * @param recipeId  id of the recipe
   * @param recipeRow recipe to create/update
   */
  public void createRecipe(RecipeId recipeId, RecipeRow recipeRow) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);

      // check if a different recipe with same name exists
      String recipeName = recipeRow.getRecipe().getRecipeName();
      RecipeRow oldRecipeWithName = getRecipeByName(table, recipeName, recipeId.getNamespace(), false);
      if (oldRecipeWithName != null) {
        throw new RecipeAlreadyExistsException(String.format("recipe with name '%s' already exists", recipeName));
      }
      saveRecipe(recipeId, recipeRow, false, table);
    });
  }

  /**
   * Update a recipe
   * @param recipeId  id of the recipe
   * @param recipeRow recipe to create/update
   */
  public void updateRecipe(RecipeId recipeId, RecipeRow recipeRow) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);

      // check if a different recipe with same name exists
      String recipeName = recipeRow.getRecipe().getRecipeName();
      RecipeRow oldRecipeWithName = getRecipeByName(table, recipeName, recipeId.getNamespace(), false);
      if (oldRecipeWithName != null
        && !oldRecipeWithName.getRecipe().getRecipeId().equals(recipeRow.getRecipe().getRecipeId())) {
        throw new RecipeAlreadyExistsException(String.format("recipe with name '%s' already exists", recipeName));
      }
      saveRecipe(recipeId, recipeRow, true, table);
    });
  }

  // Create or update a recipe
  // failIfNotFound should be false for create, and true for update
  private void saveRecipe(RecipeId recipeId, RecipeRow recipeRow, boolean failIfNotFound, StructuredTable table)
    throws IOException, RecipeAlreadyExistsException {
    RecipeRow oldRecipe = getRecipeInternal(table, recipeId, failIfNotFound);
    if (oldRecipe != null) {
      Recipe updatedRecipe = Recipe.builder(recipeRow.getRecipe())
        .setCreatedTimeMillis(oldRecipe.getRecipe().getCreatedTimeMillis()).build();
      recipeRow = RecipeRow.builder(updatedRecipe).build();
    }
    upsertRecipe(recipeId, recipeRow, table);
  }

  // Upsert recipeRow to structured table
  private void upsertRecipe(RecipeId recipeId, RecipeRow recipeRow, StructuredTable table) throws IOException {
    Collection<Field<?>> fields = getRecipeKeys(recipeId);
    fields.add(Fields.stringField(RECIPE_NAME_FIELD, recipeRow.getRecipe().getRecipeName()));
    fields.add(Fields.longField(CREATE_TIME_COL, recipeRow.getRecipe().getCreatedTimeMillis()));
    fields.add(Fields.longField(UPDATE_TIME_COL, recipeRow.getRecipe().getUpdatedTimeMillis()));
    fields.add(Fields.stringField(RECIPE_INFO_COL, GSON.toJson(recipeRow)));

    table.upsert(fields);
  }

  /**
   * Get the Recipe associated with given recipe Id
   * @param recipeId id of the recipe to fetch
   * @return {@link Recipe} metadata for given recipe id
   * @throws RecipeNotFoundException if recipe is not found
   */
  public Recipe getRecipeById(RecipeId recipeId) throws RecipeNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      RecipeRow recipeRow = getRecipeInternal(table, recipeId, true);
      return recipeRow.getRecipe();
    }, RecipeNotFoundException.class);
  }

  /**
   * Get the Recipe associated with given recipe name if it exists, else throws {@link RecipeNotFoundException}
   * @param namespace
   * @param recipeName
   * @return {@link Recipe} metadata for given recipe name if exists
   * @throws RecipeNotFoundException
   */
  public Recipe getRecipeByName(NamespaceSummary namespace, String recipeName) throws RecipeNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      RecipeRow recipeRow = getRecipeByName(table, recipeName, namespace, true);
      return recipeRow.getRecipe();
    }, RecipeNotFoundException.class);
  }

  /**
   * Get a paginated list of recipes in given namespace
   * @param request {@link RecipePageRequest} that contains necessary query parameters
   * @return {@link RecipeListResponse} that contains a page of results and nextPageToken
   */
  public RecipeListResponse listRecipes(RecipePageRequest request) {
    return TransactionRunners.run(transactionRunner, context -> {
      List<Recipe> recipes = new ArrayList<>();
      StructuredTable table = context.getTable(TABLE_ID);

      Range range = request.getScanRange();
      try (CloseableIterator<StructuredRow> iterator = table.scan(range, request.getPageSize() + 1,
                                                                  request.getSortBy(), request.getSortOrder())) {
        iterator.forEachRemaining(
          structuredRow ->
            recipes.add(GSON.fromJson(structuredRow.getString(RECIPE_INFO_COL), RecipeRow.class).getRecipe())
        );
      }
      String nextPageToken = recipes.size() > request.getPageSize() ?
        request.getNextPageToken(recipes.remove(recipes.size() - 1)) : "";

      return new RecipeListResponse(recipes, nextPageToken);
    });
  }

  /**
   * Delete the specified Recipe
   * @param recipeId id of the recipe to delete
   * @throws RecipeNotFoundException if recipe with given id is not found
   */
  public void deleteRecipe(RecipeId recipeId) throws RecipeNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      getRecipeInternal(table, recipeId, true);
      table.delete(getRecipeKeys(recipeId));
    }, RecipeNotFoundException.class);
  }

  private RecipeRow getRecipeInternal(StructuredTable table, RecipeId recipeId, boolean failIfNotFound)
    throws RecipeNotFoundException, IOException {
    Optional<StructuredRow> row = table.read(getRecipeKeys(recipeId));
    if (!row.isPresent()) {
      if (!failIfNotFound) {
        return null;
      }
      throw new RecipeNotFoundException(String.format("recipe with id '%s' does not exist", recipeId.getRecipeId()));
    }
    return GSON.fromJson(row.get().getString(RECIPE_INFO_COL), RecipeRow.class);
  }

  private RecipeRow getRecipeByName(StructuredTable table, String recipeName,
                                    NamespaceSummary summary, boolean failIfNotFound) throws IOException {
    Collection<Field<?>> keys = getNamespaceKeys(NAMESPACE_FIELD, GENERATION_COL, summary);
    keys.add(Fields.stringField(RECIPE_NAME_FIELD, recipeName));
    Range range = Range.singleton(keys);
    CloseableIterator<StructuredRow> iterator = table.scan(range, 1);
    if (!iterator.hasNext()) {
      if (!failIfNotFound) {
        return null;
      }
      throw new RecipeNotFoundException(String.format("recipe with name '%s' does not exist", recipeName));
    }
    return GSON.fromJson(iterator.next().getString(RECIPE_INFO_COL), RecipeRow.class);
  }

  private Collection<Field<?>> getRecipeKeys(RecipeId recipeId) {
    List<Field<?>> keys = new ArrayList<>(getNamespaceKeys(
      NAMESPACE_FIELD, GENERATION_COL, recipeId.getNamespace()));
    keys.add(Fields.stringField(RECIPE_ID_FIELD, recipeId.getRecipeId()));
    return keys;
  }

  // Clean up recipes - used only by unit tests
  public void clear() {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      table.deleteAll(Range.all());
    });
  }
}
