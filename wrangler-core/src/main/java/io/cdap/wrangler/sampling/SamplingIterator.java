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

package io.cdap.wrangler.sampling;

import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.Iterator;

/**
 * A simple abstract iterator which implements the remove method as unsupported operation.
 *
 * @param <T> The type of iterator data.
 */
@PublicEvolving
public abstract class SamplingIterator<T> implements Iterator<T> {
  @Override
  public void remove() {
    throw new UnsupportedOperationException("This operation is not supported.");
  }
}
