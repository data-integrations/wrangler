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
 */
@PublicEvolving
public enum TokenType implements Serializable {
  /**
   * Represents the enumerated type for the object {@code DirectiveName} type.
   * This type is associated with the token that is recognized as a directive
   * name within the recipe.
   */
  DIRECTIVE_NAME,

  /**
   * Represents the enumerated type for the object of {@code ColumnName} type.
   * This type is associated with token that represents the column as defined
   * by the grammar as :<column-name>.
   */
  COLUMN_NAME,

  /**
   * Represents the enumerated type for the object of {@code Text} type.
   * This type is associated with the token that is either enclosed within a single quote(')
   * or a double quote (") as string.
   */
  TEXT,

  /**
   * Represents the enumerated type for the object of {@code Numeric} type.
   * This type is associated with the token that is either a integer or real number.
   */
  NUMERIC,

  /**
   * Represents the enumerated type for the object of {@code Bool} type.
   * This type is associated with the token that either represents string 'true' or 'false'.
   */
  BOOLEAN,

  /**
   * Represents the enumerated type for the object of type {@code BoolList} type.
   * This type is associated with the rule that is a collection of {@code Boolean} values
   * separated by comman(,). E.g.
   * <code>
   *   ColumnName[,ColumnName]*
   * </code>
   */
  COLUMN_NAME_LIST,

  /**
   * Represents the enumerated type for the object of type {@code TextList} type.
   * This type is associated with the comma separated text represented were each text
   * is enclosed within a single quote (') or double quote (") and each text is separated
   * by comma (,). E.g.
   * <code>
   *   Text[,Text]*
   * </code>
   */
  TEXT_LIST,

  /**
   * Represents the enumerated type for the object of type {@code NumericList} type.
   * This type is associated with the collection of {@code Numeric} values separated by
   * comma(,). E.g.
   * <code>
   *   Numeric[,Numeric]*
   * </code>
   *
   */
  NUMERIC_LIST,

  /**
   * Represents the enumerated type for the object of type {@code BoolList} type.
   * This type is associated with the collection of {@code Bool} values separated by
   * comma(,). E.g.
   * <code>
   *   Boolean[,Boolean]*
   * </code>
   */
  BOOLEAN_LIST,

  /**
   * Represents the enumerated type for the object of type {@code Expression} type.
   * This type is associated with code block that either represents a condition or
   * an expression. E.g.
   * <code>
   *   exp:{ <expression || condition> }
   * </code>
   */
  EXPRESSION,

  /**
   * Represents the enumerated type for the object of type {@code Properties} type.
   * This type is associated with a collection of key and value pairs all separated
   * by a comma(,). E.g.
   * <code>
   *   prop:{ <key>=<value>[,<key>=<value>]*}
   * </code>
   */
  PROPERTIES,

  /**
   * Represents the enumerated type for the object of type {@code Ranges} types.
   * This type is associated with a collection of range represented in the form shown
   * below
   * <code>
   *   <start>:<end>=value[,<start>:<end>=value]*
   * </code>
   */
  RANGES,

  /**
   * Represents the enumerated type for the object of type {@code String} with restrictions
   * on characters that can be present in a string.
   */
  IDENTIFIER
}
