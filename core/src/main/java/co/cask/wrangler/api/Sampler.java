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

package co.cask.wrangler.api;

import co.cask.wrangler.api.annotations.PublicEvolving;

import java.util.Iterator;

/**
 * This class is interface for implementing <code>Sampler</code>. It provides
 * a set of data selected from a statistical population by the defined implementation.
 *
 * It helps create random samples of data easily.
 */
@PublicEvolving
public abstract class Sampler<T> {
  protected final static double EPSILON = 1e-5;

  protected final Iterator<T> EMPTY_ITERABLE = new SamplingIterator<T>() {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public T next() {
      return null;
    }
  };

  /**
   * Randomly sample the elements from input in sequence, and return the result iterator.
   *
   * @param input Source data
   * @return The sample result.
   */
  public abstract Iterator<T> sample(Iterator<T> input);
}

