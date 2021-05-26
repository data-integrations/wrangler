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

import java.util.Map;
import java.util.Objects;

/**
 * Plugin information
 */
public class Plugin {
  private final String name;
  private final String type;
  private final Map<String, String> properties;
  private final Artifact artifact;

  public Plugin(String name, String type, Map<String, String> properties, Artifact artifact) {
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.artifact = artifact;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public Artifact getArtifact() {
    return artifact;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Plugin that = (Plugin) o;
    return Objects.equals(name, that.name) &&
             Objects.equals(type, that.type) &&
             Objects.equals(properties, that.properties) &&
             Objects.equals(artifact, that.artifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, properties, artifact);
  }
}
