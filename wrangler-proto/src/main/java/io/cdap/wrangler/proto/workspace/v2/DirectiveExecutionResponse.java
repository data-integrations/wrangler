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

import io.cdap.wrangler.proto.workspace.WorkspaceValidationResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * V2 version of execution response
 */
public class DirectiveExecutionResponse extends ServiceResponse<Map<String, Object>> {
  private final Set<String> headers;
  private final Map<String, String> types;
  private final WorkspaceValidationResult summary;

  public DirectiveExecutionResponse(List<Map<String, Object>> values, Set<String> headers, Map<String, String> types,
                                    WorkspaceValidationResult summary) {
    super(values);
    this.headers = headers;
    this.types = types;
    this.summary = summary;
  }

  public Set<String> getHeaders() {
    return headers;
  }

  public Map<String, String> getTypes() {
    return types;
  }

  public WorkspaceValidationResult getSummary() {
    return summary;
  }
}
