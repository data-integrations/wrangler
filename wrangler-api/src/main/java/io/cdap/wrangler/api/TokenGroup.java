/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.cdap.wrangler.api;

import io.cdap.wrangler.api.parser.Token;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A group of tokens that represents a parsed directive.
 */
public final class TokenGroup {
  /** Source information for this token group. */
  private final SourceInfo info;
  
  /** List of tokens in this group. */
  private final List<Token> tokens;

  /**
   * Creates a new token group with no source information.
   */
  public TokenGroup() {
    this.info = null;
    this.tokens = new ArrayList<>();
  }

  /**
   * Creates a new token group with source information.
   * 
   * @param info Source information for this token group
   */
  public TokenGroup(final SourceInfo info) {
    this.info = info;
    this.tokens = new ArrayList<>();
  }

  /**
   * Adds a token to this group.
   * 
   * @param token Token to add
   */
  public void add(final Token token) {
    tokens.add(token);
  }

  /**
   * Returns the number of tokens in this group.
   * 
   * @return Number of tokens
   */
  public int size() {
    return tokens.size();
  }

  /**
   * Returns the token at the specified index.
   * 
   * @param i Index of the token
   * @return Token at the specified index
   */
  public Token get(final int i) {
    return tokens.get(i);
  }

  /**
   * Returns an iterator over the tokens in this group.
   * 
   * @return Iterator for the tokens
   */
  public Iterator<Token> iterator() {
    return tokens.iterator();
  }

  /**
   * Returns the source information for this token group.
   * 
   * @return Source information
   */
  public SourceInfo getSourceInfo() {
    return info;
  }

  /**
   * Returns the token at the specified index.
   *
   * @param i the index of the token to retrieve
   * @return the token at the specified index
   */
  public Token getToken(final int i) {
    return tokens.get(i);
  }
}
