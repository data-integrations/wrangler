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
    * Represents a boolean value token type.
    */
   BOOL,
 
   /**
    * Represents a list of boolean values token type.
    */
   BOOL_LIST,
 
   /**
    * Represents a column name token type.
    */
   COLUMN_NAME,
 
   /**
    * Represents a list of column names token type.
    */
   COLUMN_NAME_LIST,
 
   /**
    * Represents a numeric value token type.
    */
   NUMERIC,
 
   /**
    * Represents a list of numeric values token type.
    */
   NUMERIC_LIST,
 
   /**
    * Represents a properties token type.
    */
   PROPERTIES,
 
   /**
    * Represents a range token type.
    */
   RANGES,
 
   /**
    * Represents an expression token type.
    */
   EXPRESSION,
 
   /**
    * Represents a text value token type.
    */
   TEXT,
 
   /**
    * Represents a list of text values token type.
    */
   TEXT_LIST,
 
   /**
    * Represents the enumerated type for the object of type BYTE_SIZE.
    * This type is associated with tokens that represent byte sizes like '10KB', '2GB', etc.
    */
   BYTE_SIZE,
 
   /**
    * Represents the enumerated type for the object of type TIME_DURATION.
    * This type is associated with tokens that represent time durations like '10ms', '5m', etc.
    */
   TIME_DURATION,
 
   /**
    * Represents a boolean type.
    */
   BOOLEAN,
 
   /**
    * Represents a boolean list type.
    */
   BOOLEAN_LIST,
 
   /**
    * Represents an identifier type.
    */
   IDENTIFIER
 }
 
//End of file