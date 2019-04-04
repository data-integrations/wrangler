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

package io.cdap.wrangler.proto.bigquery;

import com.google.gson.annotations.SerializedName;

/**
 * Information about a BigQuery dataset.
 */
public class DatasetInfo {
  private final String name;
  private final String description;
  private final String location;
  private final Long created;
  @SerializedName("last-modified")
  private final Long lastModified;

  public DatasetInfo(String name, String description, String location, Long created, Long lastModified) {
    this.name = name;
    this.description = description;
    this.location = location;
    this.created = created;
    this.lastModified = lastModified;
  }
}
