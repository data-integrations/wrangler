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
 * The DataModelRequest object contains the properties for adding a DataModel to the workspace.
 */
public final class DataModelRequest {

  private final String id;
  private final Long revision;

  public DataModelRequest(String id, Long revision) {
    this.id = id;
    this.revision = revision;
  }

  public String getId() {
    return this.id;
  }

  public Long getRevision() {
    return this.revision;
  }
}
