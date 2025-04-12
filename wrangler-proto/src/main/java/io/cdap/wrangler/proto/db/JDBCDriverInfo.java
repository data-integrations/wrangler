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

package io.cdap.wrangler.proto.db;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;

/**
 * Information about a JDBC driver plugin.
 */
public class JDBCDriverInfo {
  private final String label;
  private final String version;
  private final String url;
  @SerializedName("default.port")
  private final String port;
  private final List<String> fields;
  private final Map<String, String> properties;

  public JDBCDriverInfo(String label, String version, String url, String port, List<String> fields,
                        Map<String, String> properties) {
    this.label = label;
    this.version = version;
    this.url = url;
    this.port = port;
    this.fields = fields;
    this.properties = properties;
  }
}
