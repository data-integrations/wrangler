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

import io.cdap.wrangler.api.annotations.PublicEvolving;
import java.io.Serializable;

/**
 * The TokenType class provides the enumerated types for different types of
 * tokens that are supported by the grammar.
 *
 * Each of the enumerated types specified in this class also has an associated
 * object representing it. For example, {@code DIRECTIVE_NAME} is represented by
 * the object {@code DirectiveName}.
 *
 * @see Bool
 * @see BoolList
 * @see ColumnName
 * @see ColumnNameList
 * @see DirectiveName
 * @see Numeric
 * @see NumericList
 * @see Properties
 * @see Ranges
 * @see Expression
 * @see Text
 * @see TextList
 */
@PublicEvolving
public enum TokenType implements Serializable {

  /**
   * Token for directive names.
   */
  DIRECTIVE_NAME,

  /**
   * Token for a single column name.
   */
  COLUMN_NAME,

  /**
   * Token for text strings (quoted).
   */
  TEXT,

  /**
   * Token for numeric values.
   */
  NUMERIC,

  /**
   * Token for boolean values ("true" or "false").
   */
  BOOLEAN,

  /**
   * Token for a list of column names.
   */
  COLUMN_NAME_LIST,

  /**
   * Token for a list of text values.
   */
  TEXT_LIST,

  /**
   * Token for a list of numeric values.
   */
  NUMERIC_LIST,

  /**
   * Token for a list of boolean values.
   */
  BOOLEAN_LIST,

  /**
   * Token for expressions or conditions.
   */
  EXPRESSION,

  /**
   * Token for key-value property pairs.
   */
  PROPERTIES,

  /**
   * Token for range-value mappings.
   */
  RANGES,

  /**
   * Token for identifiers with restricted characters.
   */
  IDENTIFIER,

  /**
   * Token representing a byte size (e.g., "10KB", "1MB").
   */
  BYTE_SIZE,
  /**
   * Token representing a time duration (e.g., "500ms", "2h").
   */
  TIME_DURATION
}
