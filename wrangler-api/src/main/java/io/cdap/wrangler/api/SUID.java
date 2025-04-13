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

import java.util.UUID;

/**
 * A utility class for generating session unique identifiers.
 */
public final class SUID {
  /** The length of hex digits in a UUID. */
  private static final int UUID_HEX_LENGTH = 32;

  /** The generated unique identifier. */
  private final String id;

  /**
   * Creates a new instance with a randomly generated ID.
   */
  public SUID() {
    this(generateId());
  }

  /**
   * Creates a new instance with the given ID.
   * 
   * @param id The ID to use
   */
  private SUID(final String id) {
    this.id = id;
  }

  /**
   * Generates a new unique identifier.
   * 
   * @return A new unique identifier string
   */
  private static String generateId() {
    String uuid = UUID.randomUUID().toString();
    return uuid.substring(0, UUID_HEX_LENGTH / 2);
  }

  @Override
  public String toString() {
    return id;
  }
}
