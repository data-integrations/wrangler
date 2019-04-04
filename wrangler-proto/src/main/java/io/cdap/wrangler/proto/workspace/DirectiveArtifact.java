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

import io.cdap.cdap.api.artifact.ArtifactInfo;

import java.util.Map;

/**
 * Information about an artifact that contains a directive.
 */
public class DirectiveArtifact {
  private final String name;
  private final String version;
  private final String scope;
  private final Map<String, String> properties;

  public DirectiveArtifact(ArtifactInfo artifactInfo) {
    this.name = artifactInfo.getName();
    this.version = artifactInfo.getVersion();
    this.scope = artifactInfo.getScope().name();
    this.properties = artifactInfo.getProperties();
  }

  public DirectiveArtifact(String name, String version, String scope) {
    this.name = name;
    this.version = version;
    this.scope = scope;
    this.properties = null;
  }
}
