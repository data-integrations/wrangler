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

package io.cdap.wrangler.dataset.utils;

import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.table.field.Range;

import javax.annotation.Nullable;

/**
 * An interface to implement a page request. Pagination utility class that contains relevant fields and methods
 * that describe a page.
 * @param <T> Type of resource that the page request is for.
 */
public abstract class PageRequest<T> {
  /**
   * Maximum number of results to return in a page.
   */
  protected final int pageSize;

  /**
   * Token to identify the page to retrieve. Either received from a previous page or left empty to fetch the first page.
   */
  @Nullable
  protected final String pageToken;

  /**
   * Indicates the table column name based on which results should be sorted.
   */
  protected final String sortBy;

  /**
   * Indicates the sorting order (ascending or descending).
   */
  protected final SortOrder sortOrder;

  public static final String SORT_ORDER_ASC = "asc";
  public static final String SORT_ORDER_DESC = "desc";
  public static final int PAGE_SIZE_DEFAULT = 10;

  protected PageRequest(Integer pageSize, @Nullable String pageToken, String sortBy, String sortOrder) {
    this.pageToken = pageToken;

    validateSortBy(sortBy);
    this.sortBy = sortBy == null ? getDefaultSortBy() : getSortByColumnName(sortBy);

    validatePageSize(pageSize);
    this.pageSize = pageSize == null ? PAGE_SIZE_DEFAULT : pageSize;

    validateSortOrder(sortOrder);
    this.sortOrder = (sortOrder == null || sortOrder.equals(SORT_ORDER_ASC)) ? SortOrder.ASC : SortOrder.DESC;
  }

  public int getPageSize() {
    return pageSize;
  }

  @Nullable
  public String getPageToken() {
    return pageToken;
  }

  public String getSortBy() {
    return sortBy;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }

  /**
   * Checks whether given sortBy value has a respective table column mapping.
   * @param sortBy field based on which results should be sorted
   * @throws IllegalArgumentException if the value does not have a mapping
   */
  protected abstract void validateSortBy(String sortBy) throws IllegalArgumentException;

  /**
   * Get the default table column name using which results will be sorted.
   */
  protected abstract String getDefaultSortBy();

  /**
   * Get the table column name mapped to given sortBy value.
   */
  protected abstract String getSortByColumnName(String sortBy);

  /**
   * Constructs the range used to query the storage table.
   * @return {@link Range} corresponding to page query parameters.
   */
  public abstract Range getScanRange();

  /**
   * Get the nextPageToken returned as part of response to request.
   * @param object resource returned by the request
   */
  public abstract String getNextPageToken(T object);

  protected void validatePageSize(Integer pageSize) {
    if (pageSize != null && pageSize <= 0) {
      throw new IllegalArgumentException("pageSize cannot be negative or zero.");
    }
  }

  protected void validateSortOrder(String sortOrder) {
    if (sortOrder != null && !(sortOrder.equals(SORT_ORDER_ASC) || sortOrder.equals(SORT_ORDER_DESC))) {
      throw new IllegalArgumentException(
        String.format("Invalid sortOrder '%s' specified. sortOrder must be one of: '%s' or '%s'",
                      sortOrder, SORT_ORDER_ASC, SORT_ORDER_DESC));
    }
  }
}
