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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Stage spec for a plugin
 */
public class StageSpec {
  private final Schema schema;
  private final Map<String, String> properties;

  public StageSpec(@Nullable Schema schema, Map<String, String> properties) {
    this.schema = schema;
    this.properties = properties;
  }

  @Nullable
  public Schema getSchema() {
    return schema;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
