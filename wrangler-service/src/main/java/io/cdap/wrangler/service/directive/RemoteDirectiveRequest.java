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
package io.cdap.wrangler.service.directive;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.parser.DirectiveClass;

import java.util.HashMap;
import java.util.Map;

/**
 * Request for remote execution of directives
 */
public class RemoteDirectiveRequest {

  private final String recipe;
  private final Map<String, DirectiveClass> systemDirectives;
  private final String pluginNameSpace;
  private final byte[] data;
  private final Schema inputSchema;

  RemoteDirectiveRequest(String recipe, Map<String, DirectiveClass> systemDirectives,
                         String pluginNameSpace, byte[] data, Schema inputSchema) {
    this.recipe = recipe;
    this.systemDirectives = new HashMap<>(systemDirectives);
    this.pluginNameSpace = pluginNameSpace;
    this.data = data;
    this.inputSchema = inputSchema;
  }

  public String getRecipe() {
    return recipe;
  }

  public Map<String, DirectiveClass> getSystemDirectives() {
    return systemDirectives;
  }

  public byte[] getData() {
    return data;
  }

  public String getPluginNameSpace() {
    return pluginNameSpace;
  }

  public Schema getInputSchema() {
    return inputSchema;
  }
}
