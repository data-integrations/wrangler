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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
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
 * Each of the enumerated types specified in this class also has associated
 * object representing it. e.g. {@code DIRECTIVE_NAME} is represented by the
 * object {@code DirectiveName}.
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
 * @see ByteSize
 * @see TimeDuration
 */
@PublicEvolving
public enum TokenType implements Serializable {
  /**
   * Represents the enumerated type for the object {@code DirectiveName} type.
   */
  DIRECTIVE_NAME,

  /**
   * Represents the enumerated type for the object of {@code ColumnName} type.
   */
  COLUMN_NAME,

  /**
   * Represents the enumerated type for the object of {@code Text} type.
   */
  TEXT,

  /**
   * Represents the enumerated type for the object of {@code Numeric} type.
   */
  NUMERIC,

  /**
   * Represents the enumerated type for the object of {@code Bool} type.
   */
  BOOLEAN,

  /**
   * Represents the enumerated type for the object of type {@code BoolList} type.
   */
  COLUMN_NAME_LIST,

  /**
   * Represents the enumerated type for the object of type {@code TextList} type.
   */
  TEXT_LIST,

  /**
   * Represents the enumerated type for the object of type {@code NumericList}
   * type.
   */
  NUMERIC_LIST,

  /**
   * Represents the enumerated type for the object of type {@code BoolList} type.
   */
  BOOLEAN_LIST,

  /**
   * Represents the enumerated type for the object of type {@code Expression}
   * type.
   */
  EXPRESSION,

  /**
   * Represents the enumerated type for the object of type {@code Properties}
   * type.
   */
  PROPERTIES,

  /**
   * Represents the enumerated type for the object of type {@code Ranges} type.
   */
  RANGES,

  /**
   * Represents the enumerated type for the object of type {@code String} with
   * restrictions.
   */
  IDENTIFIER,

  /**
   * Represents the enumerated type for the object of type {@code ByteSize} type.
   * This type is associated with values like "10KB", "1.5MB", etc.
   */
  BYTE_SIZE,

  /**
   * Represents the enumerated type for the object of type {@code TimeDuration}
   * type.
   * This type is associated with values like "150ms", "2s", "1.5m", etc.
   */
  TIME_DURATION
}
