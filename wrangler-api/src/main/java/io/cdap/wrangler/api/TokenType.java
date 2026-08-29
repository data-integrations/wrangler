/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.wrangler.api;

import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * Enum representing the types of tokens in the Wrangler grammar.
 */
@PublicEvolving
public enum TokenType {
  // Basic tokens
  IDENTIFIER,
  STRING,
  NUMBER,
  BOOLEAN,
  NULL,
  
  // Special tokens
  BYTE_SIZE,
  TIME_DURATION,
  
  // Operators
  PLUS,
  MINUS,
  MULTIPLY,
  DIVIDE,
  MODULO,
  ASSIGN,
  EQUALS,
  NOT_EQUALS,
  GREATER_THAN,
  LESS_THAN,
  GREATER_EQUALS,
  LESS_EQUALS,
  
  // Keywords
  AND,
  OR,
  NOT,
  IF,
  ELSE,
  FOR,
  WHILE,
  BREAK,
  CONTINUE,
  RETURN,
  
  // Delimiters
  LEFT_PAREN,
  RIGHT_PAREN,
  LEFT_BRACE,
  RIGHT_BRACE,
  LEFT_BRACKET,
  RIGHT_BRACKET,
  COMMA,
  SEMICOLON,
  DOT,
  
  // Other
  EOF,
  ERROR
} 