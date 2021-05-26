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

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * An artifact.
 */
public class Artifact {
  private final String name;
  private final String version;
  private final String scope;

  public Artifact(String name, String version, String scope) {
    this.name = name;
    this.version = version;
    this.scope = scope;
  }

  @Nullable
  public String getName() {
    return name;
  }

  @Nullable
  public String getVersion() {
    return version;
  }

  @Nullable
  public String getScope() {
    return scope;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Artifact artifact = (Artifact) o;
    return Objects.equals(name, artifact.name) &&
             Objects.equals(version, artifact.version) &&
             Objects.equals(scope, artifact.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version, scope);
  }
}
