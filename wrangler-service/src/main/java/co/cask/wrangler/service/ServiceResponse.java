/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.wrangler.service;

import java.net.HttpURLConnection;
import java.util.List;

/**
 * Response sent by spanner service methods
 * @param <T>
 */
public class ServiceResponse<T> {
  private final int httpStatusCode;
  private final String statusMessage;
  private final int count;
  private final List<T> values;

  public ServiceResponse(List<T> values) {
    this.httpStatusCode = HttpURLConnection.HTTP_OK;
    this.statusMessage = "Success";
    this.count = values.size();
    this.values = values;
  }
}
