/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.api.parser;

/**
 * Defines the possible types of tokens that can appear in directives.
 */
public enum TokenType {
  /**
   * A text value token.
   *
   * <p>Used for basic string literals.
   */
  TEXT,

  /**
   * A directive name token.
   *
   * <p>Used as the first token in a directive to identify the operation.
   */
  DIRECTIVE_NAME,

  /**
   * A column name token.
   *
   * <p>Used to reference data columns.
   */
  COLUMN_NAME,

  /**
   * A list of column names.
   *
   * <p>Used to reference multiple columns at once.
   */
  COLUMN_NAME_LIST,

  /**
   * A key-value property mapping.
   *
   * <p>Used for configuration options.
   */
  PROPERTIES,

  /**
   * A regular expression pattern.
   *
   * <p>Used for string matching and manipulation.
   */
  EXPRESSION,

  /**
   * A numeric value token.
   *
   * <p>Used for numbers and numeric calculations.
   */
  NUMERIC,

  /**
   * A list of numeric values.
   *
   * <p>Used for working with sets of numbers.
   */
  NUMERIC_LIST,

  /**
   * A list of text values.
   *
   * <p>Used for working with sets of strings.
   */
  TEXT_LIST,

  /**
   * A boolean value token.
   *
   * <p>Used for true/false conditions.
   */
  BOOLEAN,

  /**
   * A list of boolean values.
   *
   * <p>Used for sets of conditions.
   */
  BOOLEAN_LIST,

  /**
   * A duration value token.
   *
   * <p>Used for time intervals.
   */
  TIME_DURATION,

  /**
   * A byte size value token.
   *
   * <p>Used for data sizes.
   */
  BYTE_SIZE,

  /**
   * A list of number ranges.
   *
   * <p>Used for numeric intervals.
   */
  RANGES,

  /**
   * A generic identifier token.
   *
   * <p>Used for variable names.
   */
  IDENTIFIER
}
