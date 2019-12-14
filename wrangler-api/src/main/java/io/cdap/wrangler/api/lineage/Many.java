/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.wrangler.api.lineage;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.wrangler.api.parser.ColumnName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code Many} class is a helper to define many-to-one or one-to-many or many-to-many relations.
 *
 * <p>This class provides two main static methods {@code of} and {@code columns}.
 * </p>
 *
 * <code>
 *   Mutation.builder("Mapping expression")
 *    .readable("string")
 *    .relation(Many.of("col1", "col2")).build();
 *
 *   ...
 *
 *   Mutation.builder("Mapping expression")
 *    .readable("string")
 *    .relation("source", Many.columns("col3", "col4", "col5").build();
 * </code>
 *
 * @see Lineage
 * @see Mutation
 * @see Relation
 */
@Beta
public final class Many implements Serializable {
  private List<String> columns = new ArrayList<>();

  /**
   * @return a instance of <pre>List<String></pre> that returns columns associated with source or target.
   */
  public List<String> columns() {
    return columns;
  }

  /**
   * Method provides an easy way to translate ellipses parameters into a array of columns.
   *
   * @param columns list source or target columns of type {@link String}.
   * @return a instance of {@link Many}.
   */
  public static Many of(String ... columns) {
    return new Many(columns);
  }

  /**
   * List of columns as source or target.
   *
   * @param columns list source or target columns of type {@link List} of {@link String}.
   * @return a instance of {@link Many}.
   */
  public static Many of(List<String> columns) {
    return new Many(columns);
  }

  /**
   * Set of columns as source or target.
   *
   * @param columns list source or target columns of type {@linkn Set} of {@link String}.
   * @return a instance of {@link Many}.
   */
  public static Many of(Set<String> columns) {
    return new Many(columns);
  }

  /**
   * List of {@link ColumnName} specified as source or target.
   *
   * @param columns list source or target columns of type {@link List} of {@link ColumnName}.
   * @return a instance of {@link Many}.
   */
  public static Many columns(List<ColumnName> columns) {
    return new Many(columns.stream().map(ColumnName::value).collect(Collectors.toList()));
  }

  /**
   * Set of {@link ColumnName} specified as source or target.
   *
   * @param columns list source or target columns of type {@link Set} of {@link ColumnName}.
   * @return a instance of {@link Many}.
   */
  public static Many columns(Set<ColumnName> columns) {
    return new Many(columns.stream().map(ColumnName::value).collect(Collectors.toList()));
  }

  /**
   * List of {@link ColumnName} specifying source or target.
   *
   * @param columns list source or target columns of type {@link ColumnName}.
   * @return a instance of {@link Many}.
   */
  public static Many columns(ColumnName ... columns) {
    return new Many(Arrays.stream(columns).map(ColumnName::value).collect(Collectors.toList()));
  }

  /**
   * List of columns of type {@link String} specifying either source or target.
   *
   * @param columns list source or target columns of type {@link String}.
   * @return a instance of {@link Many}.
   */
  public static Many columns(String ... columns) {
    return new Many(columns);
  }

  private Many() {
    // prevent anyone from creating this object.
  }

  private Many(List<String> columns) {
    this.columns.addAll(columns);
  }

  private Many(Set<String> columns) {
    this.columns.addAll(columns);
  }

  private Many(String ... columns) {
    this.columns.addAll(Arrays.asList(columns));
  }
}
