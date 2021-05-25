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

import javax.annotation.Nullable;

/**
 * Workspace specification
 */
public class WorkspaceSpec {
  private final StageSpec source;
  private final StageSpec wrangler;

  public WorkspaceSpec(@Nullable StageSpec source, StageSpec wrangler) {
    this.source = source;
    this.wrangler = wrangler;
  }

  @Nullable
  public StageSpec getSource() {
    return source;
  }

  public StageSpec getWrangler() {
    return wrangler;
  }
}
