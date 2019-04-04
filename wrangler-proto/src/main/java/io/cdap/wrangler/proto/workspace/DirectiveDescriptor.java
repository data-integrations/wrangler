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

import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.api.plugin.PluginClass;

/**
 * Describes a directive.
 */
public class DirectiveDescriptor {
  private final String name;
  private final String description;
  private final String type;
  @SerializedName("class")
  private final String className;
  private final DirectiveArtifact artifact;

  public DirectiveDescriptor(PluginClass pluginClass, DirectiveArtifact artifact) {
    this.name = pluginClass.getName();
    this.description = pluginClass.getDescription();
    this.type = pluginClass.getType();
    this.className = pluginClass.getClassName();
    this.artifact = artifact;
  }
}
