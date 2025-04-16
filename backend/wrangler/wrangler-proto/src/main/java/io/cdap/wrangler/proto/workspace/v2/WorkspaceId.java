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

import io.cdap.cdap.api.NamespaceSummary;

import java.util.Objects;
import java.util.UUID;

/**
 * Workspace id
 */
public class WorkspaceId {
  private final NamespaceSummary namespace;
  private final String workspaceId;

  public WorkspaceId(NamespaceSummary namespace) {
    this(namespace, UUID.randomUUID().toString());
  }

  public WorkspaceId(NamespaceSummary namespace, String workspaceId) {
    this.namespace = namespace;
    this.workspaceId = workspaceId;
  }

  public NamespaceSummary getNamespace() {
    return namespace;
  }

  public String getWorkspaceId() {
    return workspaceId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WorkspaceId that = (WorkspaceId) o;
    return Objects.equals(namespace, that.namespace) &&
             Objects.equals(workspaceId, that.workspaceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, workspaceId);
  }
}
