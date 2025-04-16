/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler.proto.workspace.v2;

import java.util.Collection;
import java.util.Collections;

/**
 * Service response for v2 endpoints.
 *
 * @param <T> type of value returned by the response
 */
public class ServiceResponse<T> {
  private final String message;
  private final Integer count;
  private final Collection<T> values;

  public ServiceResponse(String message) {
    this.message = message;
    this.count = null;
    this.values = Collections.emptyList();
  }

  public ServiceResponse(T value) {
    this(Collections.singletonList(value));
  }

  public ServiceResponse(Collection<T> values) {
    this(values, "Success");
  }

  public ServiceResponse(Collection<T> values, String message) {
    this.message = message;
    this.count = values.size();
    this.values = values;
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

