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
 */

package io.cdap.wrangler.parser;

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.wrangler.registry.DirectiveScope;

import javax.annotation.Nullable;

/**
 * This class carries the class information about a directive.
 */
public class DirectiveClass {

  private final String name;
  private final DirectiveScope scope;
  private final ArtifactId artifactId;
  private final String className;

  public DirectiveClass(String name, String className, DirectiveScope scope, @Nullable ArtifactId artifactId) {
    this.name = name;
    this.className = className;
    this.scope = scope;
    this.artifactId = artifactId;
  }

  public String getName() {
    return name;
  }

  public String getClassName() {
    return className;
  }

  public DirectiveScope getScope() {
    return scope;
  }

  @Nullable
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  @Override
  public String toString() {
    return "DirectiveClass{" +
      "name='" + name + '\'' +
      ", scope=" + scope +
      ", artifactId=" + artifactId +
      ", className='" + className + '\'' +
      '}';
  }
}
