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
 * Enum defining all possible token types in the wrangler directives language.
 */
public enum TokenType {
  COLUMN_NAME,
  COLUMN_NAME_LIST,
  NUMERIC,
  NUMERIC_LIST,
  BOOLEAN,
  BOOL_LIST,
  TEXT,
  TEXT_LIST,
  BYTE_SIZE,
  TIME_DURATION,
  EXPRESSION,
  PROPERTIES,
  RANGES,
  DIRECTIVE_NAME,
  IDENTIFIER
} 