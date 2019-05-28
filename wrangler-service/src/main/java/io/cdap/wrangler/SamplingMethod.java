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

package io.cdap.wrangler;

import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * This class {@link SamplingMethod} defines different types of sampling methods available.
 */
@PublicEvolving
public enum SamplingMethod {
  NONE("none"),
  FIRST("first"),
  POISSON("poisson"),
  BERNOULLI("bernoulli"),
  RESERVOIR("reservoir");

  private String method;

  SamplingMethod(String method) {
    this.method = method;
  }

  /**
   * @return String representation of enum.
   */
  public String getMethod() {
    return method;
  }

  /**
   * Provided the sampling method as string, determine the enum type of {@link SamplingMethod}.
   *
   * @param from string for which the {@link SamplingMethod} instance need to be determined.
   * @return if there is a string representation of enum, else null.
   */
  public static SamplingMethod fromString(String from) {
    if (from == null || from.isEmpty()) {
      return null;
    }
    for (SamplingMethod method : SamplingMethod.values()) {
      if (method.method.equalsIgnoreCase(from)) {
        return method;
      }
    }
    return null;
  }
}
