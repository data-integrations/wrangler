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

  public String getNextPageToken() {
    return nextPageToken;
  }
}
