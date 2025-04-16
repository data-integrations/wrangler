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

package io.cdap.wrangler.proto;

import java.util.Objects;

/**
 * Defines the sampling specification of the {@link Request}
 */
public final class Sampling {
  // Sampling method.
  private final String method;

  // Seeding capability for sampling.
  private final Integer seed;

  // Number of records to be read.
  private final Integer limit;

  public Sampling(String method, Integer seed, Integer limit) {
    this.method = method;
    this.seed = seed;
    this.limit = limit;
  }

  /**
   * @return Method for sampling data.
   */
  public String getMethod() {
    return method;
  }

  /**
   * @return Sampling seed.
   */
  public Integer getSeed() {
    if (seed != null) {
      return seed;
    } else {
      return 1;
    }
  }

  /**
   * @return Number of records to be read before applying directives.
   */
  public Integer getLimit() {
    if (limit != null) {
      return limit;
    } else {
      return 100;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Sampling sampling = (Sampling) o;
    return Objects.equals(method, sampling.method) &&
      Objects.equals(seed, sampling.seed) &&
      Objects.equals(limit, sampling.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(method, seed, limit);
  }
}
