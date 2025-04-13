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

/**
 * A container for three related values.
 *
 * @param <T1> Type of first value
 * @param <T2> Type of second value
 * @param <T3> Type of third value
 */
public class Triplet<T1, T2, T3> {
  /** The first value in the triplet. */
  private final T1 first;

  /** The second value in the triplet. */
  private final T2 second;

  /** The third value in the triplet. */
  private final T3 third;

  /**
   * Creates a new triplet.
   *
   * @param first The first value
   * @param second The second value
   * @param third The third value
   */
  public Triplet(final T1 first, final T2 second, final T3 third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  /**
   * Gets the first value.
   *
   * @return The first value
   */
  public T1 getFirst() {
    return first;
  }

  /**
   * Gets the second value.
   *
   * @return The second value
   */
  public T2 getSecond() {
    return second;
  }

  /**
   * Gets the third value.
   *
   * @return The third value
   */
  public T3 getThird() {
    return third;
  }
}
