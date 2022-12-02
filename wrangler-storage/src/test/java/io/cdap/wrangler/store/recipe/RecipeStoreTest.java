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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.wrangler.dataset.recipe.RecipeAlreadyExistsException;
import io.cdap.wrangler.dataset.recipe.RecipeNotFoundException;
import io.cdap.wrangler.dataset.recipe.RecipePageRequest;
import io.cdap.wrangler.dataset.recipe.RecipeRow;
import io.cdap.wrangler.proto.recipe.v2.Recipe;
import io.cdap.wrangler.proto.recipe.v2.RecipeId;
import io.cdap.wrangler.proto.recipe.v2.RecipeListResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.cdap.wrangler.dataset.recipe.RecipePageRequest.SORT_BY_UPDATE_TIME;
import static io.cdap.wrangler.dataset.utils.PageRequest.SORT_ORDER_DESC;
import static io.cdap.wrangler.store.recipe.RecipeStore.RECIPE_TABLE_SPEC;

public class RecipeStoreTest extends SystemAppTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);
  private static RecipeStore store;

  @BeforeClass
  public static void setupTest() throws Exception {
    getStructuredTableAdmin().createOrUpdate(RECIPE_TABLE_SPEC);
    store = new RecipeStore(getTransactionRunner());
  }

  @After
  public void cleanupTest() {
    store.clear();
  }

  @Test
  public void testSaveAndGetNewRecipe() {
    NamespaceSummary summary = new NamespaceSummary("n1", "", 10L);
    RecipeId recipeId = RecipeId.builder(summary).build();
    ImmutableList<String> directives = ImmutableList.of("dir1", "dir2");
    Recipe recipe = Recipe.builder(recipeId)
      .setRecipeName("dummy name")
      .setDescription("dummy description")
      .setCreatedTimeMillis(100L)
      .setUpdatedTimeMillis(100L)
      .setDirectives(directives)
      .build();
    RecipeRow recipeRow = RecipeRow.builder(recipe).build();
    store.saveRecipe(recipeId, recipeRow);

    Recipe savedRecipeRow = store.getRecipe(recipeId);
    Assert.assertEquals(recipeRow.getRecipe(), savedRecipeRow);
  }

  @Test(expected = RecipeAlreadyExistsException.class)
  public void testSaveRecipeWithDuplicateName() {
    NamespaceSummary summary = new NamespaceSummary("n1", "", 10L);
    RecipeId recipeId = RecipeId.builder(summary).build();
    RecipeRow recipeRow = RecipeRow.builder(
      Recipe.builder(recipeId).setRecipeName("duplicate name").build()).build();
    store.saveRecipe(recipeId, recipeRow);

    RecipeId newRecipeId = RecipeId.builder(summary).build();
    RecipeRow newRecipeRow = RecipeRow.builder(
      Recipe.builder(newRecipeId).setRecipeName("duplicate name").build()).build();
    store.saveRecipe(newRecipeId, newRecipeRow);
  }

  @Test(expected = RecipeNotFoundException.class)
  public void testGetRecipeDoesNotExist() {
    NamespaceSummary summary = new NamespaceSummary("n100", "", 40L);
    RecipeId recipeId = RecipeId.builder(summary).setRecipeId("non-existent-recipe-id").build();
    store.getRecipe(recipeId);
  }

  @Test
  public void testListRecipesDefault() {
    NamespaceSummary summary = new NamespaceSummary("n1", "", 10L);
    RecipeId recipeId1 = RecipeId.builder(summary).build();
    RecipeId recipeId2 = RecipeId.builder(summary).build();
    RecipeRow recipeRow1 = RecipeRow.builder(Recipe.builder(recipeId1).setRecipeName("xyz").build()).build();
    RecipeRow recipeRow2 = RecipeRow.builder(Recipe.builder(recipeId2).setRecipeName("abc").build()).build();

    store.saveRecipe(recipeId1, recipeRow1);
    store.saveRecipe(recipeId2, recipeRow2);

    // Assuming default values for query parameters
    RecipePageRequest pageRequest = RecipePageRequest.builder(summary).build();
    RecipeListResponse response = store.listRecipes(pageRequest);
    List<Recipe> values = (List<Recipe>) response.getValues();

    // Check whether values are sorted in alphabetical order by recipeName
    Assert.assertEquals(2, (int) response.getCount());
    Assert.assertEquals(values.get(0), recipeRow2.getRecipe());
    Assert.assertEquals(values.get(1), recipeRow1.getRecipe());
  }

  @Test
  public void testListRecipesSortByUpdated() {
    NamespaceSummary summary = new NamespaceSummary("n1", "", 10L);
    RecipeId recipeId1 = RecipeId.builder(summary).build();
    RecipeId recipeId2 = RecipeId.builder(summary).build();
    RecipeRow recipeRow1 = RecipeRow.builder(
      Recipe.builder(recipeId1).setRecipeName("first").setUpdatedTimeMillis(100L).build()).build();
    RecipeRow recipeRow2 = RecipeRow.builder(
      Recipe.builder(recipeId2).setRecipeName("second").setUpdatedTimeMillis(200L).build()).build();

    store.saveRecipe(recipeId1, recipeRow1);
    store.saveRecipe(recipeId2, recipeRow2);

    RecipePageRequest pageRequest = RecipePageRequest.builder(summary)
      .setSortBy(SORT_BY_UPDATE_TIME)
      .setSortOrder(SORT_ORDER_DESC)
      .build();
    RecipeListResponse response = store.listRecipes(pageRequest);
    List<Recipe> values = (List<Recipe>) response.getValues();

    // Check whether values are sorted in descending order by updatedTime
    Assert.assertEquals(2, (int) response.getCount());
    Assert.assertEquals(values.get(0), recipeRow2.getRecipe());
    Assert.assertEquals(values.get(1), recipeRow1.getRecipe());
  }

  @Test
  public void testListRecipesPagination() {
    NamespaceSummary summary = new NamespaceSummary("n1", "", 10L);
    RecipeId recipeId1 = RecipeId.builder(summary).build();
    RecipeId recipeId2 = RecipeId.builder(summary).build();
    RecipeId recipeId3 = RecipeId.builder(summary).build();
    RecipeRow recipeRow1 = RecipeRow.builder(Recipe.builder(recipeId1).setRecipeName("recipe 1").build()).build();
    RecipeRow recipeRow2 = RecipeRow.builder(Recipe.builder(recipeId2).setRecipeName("recipe 2").build()).build();
    RecipeRow recipeRow3 = RecipeRow.builder(Recipe.builder(recipeId3).setRecipeName("recipe 3").build()).build();

    store.saveRecipe(recipeId1, recipeRow1);
    store.saveRecipe(recipeId2, recipeRow2);
    store.saveRecipe(recipeId3, recipeRow3);

    RecipePageRequest pageRequest1 = RecipePageRequest.builder(summary).setPageSize(2).build();
    RecipeListResponse page1 = store.listRecipes(pageRequest1);
    List<Recipe> values = (List<Recipe>) page1.getValues();

    Assert.assertEquals(2, (int) page1.getCount());
    Assert.assertEquals(values.get(0), recipeRow1.getRecipe());
    Assert.assertEquals(values.get(1), recipeRow2.getRecipe());
    Assert.assertEquals(page1.getNextPageToken(), recipeRow3.getRecipe().getRecipeName());

    // Fetching the next page
    RecipePageRequest pageRequest2 = RecipePageRequest.builder(summary).setPageToken(page1.getNextPageToken()).build();
    RecipeListResponse page2 = store.listRecipes(pageRequest2);
    values = (List<Recipe>) page2.getValues();

    Assert.assertEquals(1, (int) page2.getCount());
    Assert.assertEquals(values.get(0), recipeRow3.getRecipe());
    Assert.assertEquals(page2.getNextPageToken(), "");
  }
}
