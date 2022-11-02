package io.cdap.wrangler.proto.recipe.v2;

import io.cdap.wrangler.proto.ServiceResponse;

import java.util.List;

/**
 * Represents a paginated list of recipes as a response.
 */
public class RecipeListResponse extends ServiceResponse<Recipe> {
  String nextPageToken;

  public RecipeListResponse(List<Recipe> recipes, String nextPageToken) {
    super(recipes);
    this.nextPageToken = nextPageToken;
  }
}
