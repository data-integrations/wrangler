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
package io.cdap.wrangler.store.recipe;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.dataset.recipe.RecipeNotFoundException;
import io.cdap.wrangler.dataset.recipe.RecipeRow;
import io.cdap.wrangler.proto.recipe.v2.Recipe;
import io.cdap.wrangler.proto.recipe.v2.RecipeId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Recipe store for v2 endpoint
 */
public class RecipeStore {
  private static final StructuredTableId TABLE_ID = new StructuredTableId("recipes_store");
  private static final String NAMESPACE_FIELD = "namespace";
  private static final String GENERATION_COL = "generation";
  private static final String RECIPE_ID_FIELD = "recipe_id";
  private static final String RECIPE_NAME_FIELD = "recipe_name";
  private static final String CREATE_TIME_COL = "create_time";
  private static final String UPDATE_TIME_COL = "update_time";
  private static final String RECIPE_INFO_COL = "recipe_info";

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
      .withIndexes(RECIPE_NAME_FIELD)
      .build();

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  private final TransactionRunner transactionRunner;

  public RecipeStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Create a new recipe
   * @param recipeId id of the recipe
   * @param recipeRow recipe to create/update
   */
  public void saveRecipe(RecipeId recipeId, RecipeRow recipeRow) {
    saveRecipe(recipeId, recipeRow, false);
  }

  /**
   * Get the Recipe associated with given recipe id
   * @param recipeId id of recipe to look up
   * @return recipe metadata for given recipe id
   * @throws RecipeNotFoundException if recipe is not found
   */
  public Recipe getRecipe(RecipeId recipeId) throws RecipeNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      RecipeRow recipeRow = getRecipeInternal(table, recipeId, true);
      return recipeRow.getRecipe();
      }, RecipeNotFoundException.class);
  }

  /**
   * Delete the given Recipe
   * @param recipeId id of the recipe to delete
   * @throws RecipeNotFoundException
   */
  public void deleteRecipe(RecipeId recipeId) throws RecipeNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      getRecipeInternal(table, recipeId, true);
      table.delete(getRecipeKeys(recipeId));
    }, RecipeNotFoundException.class);
  }

  // Create or update a recipe
  private void saveRecipe(RecipeId recipeId, RecipeRow recipeRow, boolean failIfNotFound) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(TABLE_ID);
      RecipeRow oldRecipe = getRecipeInternal(table, recipeId, failIfNotFound);
      RecipeRow newRecipe = recipeRow;

      if (oldRecipe != null) {
        Recipe updatedRecipe = Recipe.builder(newRecipe.getRecipe())
          .setCreatedTimeMillis(oldRecipe.getRecipe().getCreatedTimeMillis()).build();
        newRecipe = RecipeRow.builder(updatedRecipe).build();
      }
      Collection<Field<?>> fields = getRecipeKeys(recipeId);
      fields.add(Fields.longField(CREATE_TIME_COL, newRecipe.getRecipe().getCreatedTimeMillis()));
      fields.add(Fields.longField(UPDATE_TIME_COL, newRecipe.getRecipe().getUpdatedTimeMillis()));
      fields.add(Fields.stringField(RECIPE_INFO_COL, GSON.toJson(newRecipe)));

      table.upsert(fields);
    });
  }

  private RecipeRow getRecipeInternal(StructuredTable table, RecipeId recipeId, boolean failIfNotFound)
    throws RecipeNotFoundException, IOException {
    Optional<StructuredRow> row = table.read(getRecipeKeys(recipeId));
    if (!row.isPresent()) {
      if (!failIfNotFound) {
        return null;
      }
      throw new RecipeNotFoundException(String.format("Recipe %s does not exist", recipeId.getRecipeId()));
    }
    return GSON.fromJson(row.get().getString(RECIPE_INFO_COL), RecipeRow.class);
  }

  private Collection<Field<?>> getRecipeKeys(RecipeId recipeId) {
    List<Field<?>> keys = new ArrayList<>(getNamespaceKeys(recipeId.getNamespace()));
    keys.add(Fields.stringField(RECIPE_ID_FIELD, recipeId.getRecipeId()));
    return keys;
  }

  private Collection<Field<?>> getNamespaceKeys(NamespaceSummary namespace) {
    List<Field<?>> keys = new ArrayList<>();
    keys.add(Fields.stringField(NAMESPACE_FIELD, namespace.getName()));
    keys.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    return keys;
  }
}
