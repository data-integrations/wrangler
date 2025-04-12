/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import java.util.Collection;
import java.util.Collections;

/**
 * Service response.
 *
 * @param <T> type of value returned by the response
 */
public class ServiceResponse<T> {
  private final String message;
  private final Integer count;
  private final Collection<T> values;
  // TODO: (CDAP-14652) see if this is used by UI. It should be a boolean and not a string...
  private final String truncated;

  public ServiceResponse(String message) {
    this.message = message;
    this.count = null;
    this.values = Collections.emptyList();
    this.truncated = null;
  }

  public ServiceResponse(T value) {
    this(Collections.singletonList(value));
  }

  public ServiceResponse(Collection<T> values) {
    this(values, false);
  }

  public ServiceResponse(Collection<T> values, boolean truncated) {
    this(values, truncated, "Success");
  }

  public ServiceResponse(Collection<T> values, boolean truncated, String message) {
    this.message = message;
    this.count = values.size();
    this.values = values;
    this.truncated = Boolean.toString(truncated);
  }

  public String getMessage() {
    return message;
  }

  public Integer getCount() {
    return count;
  }

  public Collection<T> getValues() {
    return values;
  }
}
