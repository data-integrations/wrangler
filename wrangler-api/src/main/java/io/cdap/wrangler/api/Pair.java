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
 * A pair consisting of two elements - first & second.
 *
 * This class provides immutable access to elements of the pair.
 *
 * @param <F> type of the first element
 * @param <S> type of the second element
 */
public final class Pair<F, S> {
  private final F first;
  private final S second;

  public Pair(F first, S second) {
    this.first = first;
    this.second = second;
  }

  /**
   * @return First element of the pair.
   */
  public F getFirst() {
    return first;
  }

  /**
   * @return Second element of the pair.
   */
  public S getSecond() {
    return second;
  }
}
