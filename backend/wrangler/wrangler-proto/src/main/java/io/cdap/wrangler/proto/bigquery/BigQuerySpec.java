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
import io.cdap.wrangler.proto.PluginSpec;

/**
 * Plugin specification for a BigQuery pipeline source.
 *
 * TODO: (CDAP-14652) clean up this API. There is no reason for this class to exist.
 */
public class BigQuerySpec {
  @SerializedName("BigQueryTable")
  private final PluginSpec spec;

  public BigQuerySpec(PluginSpec spec) {
    this.spec = spec;
  }
}
