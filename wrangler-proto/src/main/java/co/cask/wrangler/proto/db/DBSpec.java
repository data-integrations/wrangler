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

package co.cask.wrangler.proto.db;

import co.cask.wrangler.proto.PluginSpec;
import com.google.gson.annotations.SerializedName;

/**
 * Specification for an DB pipeline source plugin.
 *
 * TODO: (CDAP-14652) clean up this API. There is no reason for this class to exist.
 */
public class DBSpec {
  @SerializedName("Database")
  private final PluginSpec spec;

  public DBSpec(PluginSpec spec) {
    this.spec = spec;
  }
}
