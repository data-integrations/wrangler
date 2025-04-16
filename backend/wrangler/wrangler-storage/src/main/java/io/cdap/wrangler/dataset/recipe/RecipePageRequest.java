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

package io.cdap.wrangler.dataset.recipe;

import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.wrangler.dataset.utils.PageRequest;
import io.cdap.wrangler.proto.recipe.v2.Recipe;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.cdap.cdap.spi.data.table.field.Range.Bound.INCLUSIVE;
import static io.cdap.wrangler.store.recipe.RecipeStore.GENERATION_COL;
import static io.cdap.wrangler.store.recipe.RecipeStore.NAMESPACE_FIELD;
import static io.cdap.wrangler.store.recipe.RecipeStore.RECIPE_NAME_FIELD;
import static io.cdap.wrangler.store.recipe.RecipeStore.UPDATE_TIME_COL;
import static io.cdap.wrangler.store.utils.Stores.getNamespaceKeys;

/**
 * Represents a request to fetch a page of Recipes.
 */
public class RecipePageRequest extends PageRequest<Recipe> {
  public static final String SORT_BY_NAME = "name";
  public static final String SORT_BY_UPDATE_TIME = "updated";

  private static final Map<String, String> sortByMap = createSortByMap();

  private final NamespaceSummary namespace;

  protected RecipePageRequest(Integer pageSize, String pageToken, String sortBy,
                            String sortOrder, NamespaceSummary namespace) {
    super(pageSize, pageToken, sortBy, sortOrder);
    this.namespace = namespace;
    validatePageTokenDataType(this.getPageToken(), this.getSortBy());
  }

  public NamespaceSummary getNamespace() {
    return namespace;
  }

  @Override
  public void validateSortBy(String sortBy) {
    if (sortBy != null && !sortByMap.containsKey(sortBy)) {
      throw new IllegalArgumentException(
        String.format("Invalid sortBy '%s' specified. sortBy must be one of: '%s' or '%s'",
                      sortBy, SORT_BY_NAME, SORT_BY_UPDATE_TIME));
    }
  }

  @Override
  protected String getDefaultSortBy() {
    return RECIPE_NAME_FIELD;
  }

  @Override
  protected String getSortByColumnName(String sortBy) {
    return sortByMap.get(sortBy);
  }

  @Override
  public Range getScanRange() {
    Collection<Field<?>> begin = getNamespaceKeys(NAMESPACE_FIELD, GENERATION_COL, namespace);
    Collection<Field<?>> end = getNamespaceKeys(NAMESPACE_FIELD, GENERATION_COL, namespace);

    // If pageToken has a value, add the respective field (column, value) to the range to filter results
    if (getPageToken() != null) {
      Field<?> sortByField = getSortByField();

      if (getSortOrder().equals(SortOrder.ASC)) {
        begin.add(sortByField);
      } else {
        end.add(sortByField);
      }
    }

    return Range.create(begin, INCLUSIVE, end, INCLUSIVE);
  }

  @Override
  public String getNextPageToken(Recipe recipe) {
    String nextPageToken;
    switch (getSortBy()) {
      case RECIPE_NAME_FIELD:
        nextPageToken = recipe.getRecipeName();
        break;
      case UPDATE_TIME_COL:
        nextPageToken = String.valueOf(recipe.getUpdatedTimeMillis());
        break;
      default:
        throw new IllegalArgumentException(
          String.format("Invalid sortBy field '%s' is not mapped to any field in Recipe to return as a pageToken.",
                        getSortBy()));
    }
    return nextPageToken;
  }

  private static Map<String, String> createSortByMap() {
    Map<String, String> sortByMap = new HashMap<>();
    sortByMap.put(SORT_BY_NAME, RECIPE_NAME_FIELD);
    sortByMap.put(SORT_BY_UPDATE_TIME, UPDATE_TIME_COL);
    return sortByMap;
  }

  private void validatePageTokenDataType(String pageToken, String sortBy) {
    if (pageToken != null && sortBy.equals(UPDATE_TIME_COL)) {
      try {
        Long.parseLong(pageToken);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
          "pageToken value is of invalid data type: expected 'long' value for sortBy 'updated'");
      }
    }
  }

  private Field<?> getSortByField() {
    Field<?> sortByField;
    switch (getSortBy()) {
      case RECIPE_NAME_FIELD:
        sortByField = Fields.stringField(RECIPE_NAME_FIELD, getPageToken());
        break;
      case UPDATE_TIME_COL:
        sortByField = Fields.longField(UPDATE_TIME_COL, Long.parseLong(Objects.requireNonNull(getPageToken())));
        break;
      default:
        throw new IllegalArgumentException(
          String.format("Invalid sortBy field '%s' is not mapped to any Recipe store column name.",
                        getSortBy()));
    }
    return sortByField;
  }

  public static Builder builder(NamespaceSummary namespace) {
    return new Builder(namespace);
  }

  /**
   * Creates a {@link RecipePageRequest} object
   */
  public static class Builder {
    private final NamespaceSummary namespace;
    private Integer pageSize;
    private String pageToken;
    private String sortBy;
    private String sortOrder;

    Builder(NamespaceSummary namespace) {
      this.namespace = namespace;
    }

    public Builder setPageSize(Integer pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public Builder setPageToken(String pageToken) {
      this.pageToken = pageToken;
      return this;
    }

    public Builder setSortBy(String sortBy) {
      this.sortBy = sortBy;
      return this;
    }

    public Builder setSortOrder(String sortOrder) {
      this.sortOrder = sortOrder;
      return this;
    }

    public RecipePageRequest build() {
      return new RecipePageRequest(pageSize, pageToken, sortBy, sortOrder, namespace);
    }
  }
}
