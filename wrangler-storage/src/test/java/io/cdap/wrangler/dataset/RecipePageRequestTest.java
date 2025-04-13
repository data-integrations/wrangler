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

/**
 * Tests for the RecipePageRequest class, including validation and range calculations
 */
public class RecipePageRequestTest {
  
  /**
   * Tests the valid case of getting scan range for a first page request
   */
  @Test
  public void testGetRangeForFirstPage() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest pageRequest = RecipePageRequest.builder(namespace).build();
    Range range = pageRequest.getScanRange();
    Collection<Field<?>> fields = getNamespaceKeys(NAMESPACE_FIELD, GENERATION_COL, namespace);
    Assert.assertEquals(range, Range.create(fields, INCLUSIVE, fields, INCLUSIVE));
  }

  /**
   * Tests validation for invalid page token format
   */
  @Test(expected = IllegalArgumentException.class)
  public void testPageRequestWithInvalidPageToken() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest.builder(namespace)
      .setSortBy(SORT_BY_UPDATE_TIME)
      .setPageToken("abc123")
      .build();
  }
  
  /**
   * Tests validation for invalid sort by field
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSortBy() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest.builder(namespace)
      .setSortBy("invalid-sortBy")
      .build();
  }

  /**
   * Tests validation for invalid sort order value
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSortOrder() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest.builder(namespace)
      .setSortOrder("invalid-sortOrder")
      .build();
  }

  /**
   * Tests validation for invalid page size (must be positive)
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPageSize() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest.builder(namespace)
      .setPageSize(0)
      .build();
  }

  /**
   * Tests validation for non-matching namespace provided in the page token.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testPageRequestWithWrongNamespace() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    // Create a page token that contains a different namespace than the one provided
    RecipePageRequest.builder(namespace)
      .setSortBy(SORT_BY_UPDATE_TIME)
      .setPageToken("n2:100:asc")
      .build();
  }

  /**
   * Tests the valid case of getting scan range for a request with a page token.
   */
  @Test
  public void testGetRangeWithPageToken() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    String validToken = "n1:100:asc";
    RecipePageRequest pageRequest = RecipePageRequest.builder(namespace)
      .setSortBy(SORT_BY_UPDATE_TIME)
      .setPageToken(validToken)
      .build();
    
    Range range = pageRequest.getScanRange();
    Assert.assertNotNull(range);
    // Additional assertions can be added based on expected range behavior
  }

  /**
   * Tests to ensure default values are set correctly when not specified.
   */
  @Test
  public void testDefaultValues() {
    NamespaceSummary namespace = new NamespaceSummary("n1", "", 10L);
    RecipePageRequest pageRequest = RecipePageRequest.builder(namespace).build();
    
    // Verify default values are correctly set
    Assert.assertNotNull(pageRequest.getSortBy());
    Assert.assertNotNull(pageRequest.getSortOrder());
    Assert.assertTrue(pageRequest.getPageSize() > 0);
  }
}
