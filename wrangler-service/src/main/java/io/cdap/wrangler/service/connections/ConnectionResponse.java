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

package io.cdap.wrangler.service.connections;

import com.google.gson.annotations.SerializedName;
import io.cdap.wrangler.proto.ServiceResponse;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Response sent by listing connections endpoint
 * @param <T>
 */
public class ConnectionResponse<T> extends ServiceResponse {
  // default connection to show in DataPrep UI
  @SerializedName("default")
  String defaultConnection;

  public ConnectionResponse(List<T> values, @Nullable String defaultConnectionId) {
    super(values);
    this.defaultConnection = defaultConnectionId;
  }
}
