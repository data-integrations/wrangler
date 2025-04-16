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

package io.cdap.wrangler.dataset;

import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.wrangler.dataset.recipe.RecipePageRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

import static io.cdap.cdap.spi.data.table.field.Range.Bound.INCLUSIVE;
import static io.cdap.wrangler.dataset.recipe.RecipePageRequest.SORT_BY_UPDATE_TIME;
import static io.cdap.wrangler.store.recipe.RecipeStore.GENERATION_COL;
import static io.cdap.wrangler.store.recipe.RecipeStore.NAMESPACE_FIELD;
import static io.cdap.wrangler.store.utils.Stores.getNamespaceKeys;

public class RecipePageRequestTest {
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSortBy() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest.builder(namespace).setSortBy("invalid-sortBy").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSortOrder() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest.builder(namespace).setSortOrder("invalid-sortOrder").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPageSize() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest.builder(namespace).setPageSize(0).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPageRequestWithInvalidPageToken() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest.builder(namespace).setSortBy(SORT_BY_UPDATE_TIME).setPageToken("abc123").build();
  }

  @Test
  public void testGetRangeForFirstPage() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest pageRequest = RecipePageRequest.builder(namespace).build();
    Range range = pageRequest.getScanRange();
    Collection<Field<?>> fields = getNamespaceKeys(NAMESPACE_FIELD, GENERATION_COL, namespace);
    Assert.assertEquals(range, Range.create(fields, INCLUSIVE, fields, INCLUSIVE));
  }
}
