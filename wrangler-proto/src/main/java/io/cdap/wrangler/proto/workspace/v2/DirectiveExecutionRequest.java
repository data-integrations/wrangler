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

import io.cdap.wrangler.proto.workspace.v2.UserDefinedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Directive execution request for v2 endpoint
 */
public class DirectiveExecutionRequest {
  private final List<String> directives;
  private final int limit;
  private final Map<String, UserDefinedAction> nullabilityMap;


  public DirectiveExecutionRequest(List<String> directives, int limit,
      Map<String, UserDefinedAction> nullabilityMap) {
    this.directives = directives;
    this.limit = limit;
    this.nullabilityMap = nullabilityMap;
  }

  public int getLimit() {
    return limit;
  }

  public List<String> getDirectives() {
    return directives == null ? Collections.emptyList() : directives;
  }

  public Map<String, UserDefinedAction> getNullabilityMap() {

    return nullabilityMap == null ? Collections.emptyMap() : nullabilityMap;
  }
}
