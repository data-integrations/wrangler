/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.proto;

/**
 * Defines the sampling specification of the {@link Request}
 */
public final class Sampling {
  // Sampling method.
  private String method;

  // Seeding capability for sampling.
  private Integer seed;

  // Number of records to be read.
  private Integer limit;

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
}
