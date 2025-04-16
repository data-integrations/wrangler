/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.wrangler.proto.workspace;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Statistics about the result of executing directives on a workspace.
 */
public class ColumnStatistics {
  private final Map<String, Float> general;
  private final Map<String, Float> types;

  public ColumnStatistics(@Nullable Map<String, Float> general, @Nullable Map<String, Float> types) {
    this.general = general;
    this.types = types;
  }

  @Nullable
  public Map<String, Float> getGeneral() {
    return general;
  }

  @Nullable
  public Map<String, Float> getTypes() {
    return types;
  }
}
