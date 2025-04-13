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

package io.cdap.wrangler.api;

import java.util.ArrayList;
import java.util.List;

/**
 * A group of tokens with their associated source information.
 */
public class TokenGroup {
  /** The source information for this token group. */
  private final SourceInfo info;

  /** The list of tokens in this group. */
  private final List<String> tokens;

  /**
   * Creates an empty token group with no source information.
   */
  public TokenGroup() {
    this(null);
  }

  /**
   * Creates a token group with source information.
   *
   * @param info The source information for this group
   */
  public TokenGroup(final SourceInfo info) {
    this.info = info;
    this.tokens = new ArrayList<>();
  }

  /**
   * Adds a token to this group.
   *
   * @param token The token to add
   */
  public final void addToken(final String token) {
    tokens.add(token);
  }

  /**
   * Gets the source information for this group.
   *
   * @return The source information, or null if not available
   */
  public final SourceInfo getInfo() {
    return info;
  }

  /**
   * Gets a token at the specified index.
   *
   * @param i The index of the token to get
   * @return The token at the specified index
   */
  public final String getToken(final int i) {
    return tokens.get(i);
  }

  /**
   * Gets the number of tokens in this group.
   *
   * @return The number of tokens
   */
  public final int size() {
    return tokens.size();
  }

  /**
   * Gets all tokens in this group.
   *
   * @return List of all tokens
   */
  public final List<String> getTokens() {
    return tokens;
  }
}
