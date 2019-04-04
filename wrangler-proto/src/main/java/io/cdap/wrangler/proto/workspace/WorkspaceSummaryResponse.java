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

/**
 * Summary of executing directives on a workspace.
 */
public class WorkspaceSummaryResponse {
  private final int status;
  private final int count;
  private final String message;
  private final WorkspaceValidationResult values;

  public WorkspaceSummaryResponse(WorkspaceValidationResult values) {
    this.status = 200;
    this.count = 2;
    this.message = "Success";
    this.values = values;
  }
}
