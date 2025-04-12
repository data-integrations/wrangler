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

import io.cdap.cdap.etl.api.connector.SampleRequest;

/**
 * Creation request for a workspace
 */
public class WorkspaceCreationRequest {
  private final String connection;
  private final String connectionType;
  private final SampleRequest sampleRequest;

  public WorkspaceCreationRequest(String connection, String connectionType, SampleRequest sampleRequest) {
    this.connection = connection;
    this.connectionType = connectionType;
    this.sampleRequest = sampleRequest;
  }

  public String getConnection() {
    return connection;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public SampleRequest getSampleRequest() {
    return sampleRequest;
  }
}
