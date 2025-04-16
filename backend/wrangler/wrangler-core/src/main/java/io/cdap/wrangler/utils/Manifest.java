/*
 *  Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.utils;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Contains the Schemas Manifest for standard schemas.
 */
public final class Manifest implements Serializable {

  private final Map<String, Standard> standards;

  public Manifest(Map<String, Standard> standards) {
    this.standards = Collections.unmodifiableMap(new HashMap<>(standards));
  }

  public Map<String, Standard> getStandards() {
    return standards;
  }

  /**
   * Contains manifest information for a single standard specification.
   */
  public static final class Standard {

    private final String hash;
    private final String format;

    public Standard(String hash, String format) {
      this.hash = hash;
      this.format = format;
    }

    public String getHash() {
      return hash;
    }

    public String getFormat() {
      return format;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      // This may cause issues if this class is loaded by custom CDF class loaders.
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Standard standard = (Standard) o;
      return Objects.equals(hash, standard.hash) && Objects.equals(format, standard.format);
    }

    @Override
    public int hashCode() {
      return Objects.hash(hash, format);
    }

    @Override
    public String toString() {
      return "Standard{" + "hash='" + hash + '\'' + ", format='" + format + '\'' + '}';
    }
  }
}
