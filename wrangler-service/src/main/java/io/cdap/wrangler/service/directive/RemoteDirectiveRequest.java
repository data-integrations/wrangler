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

import java.util.List;

/**
 * Request for remote execution of directives
 */
public class RemoteDirectiveRequest {

  private final List<String> directives;
  private final String pluginNameSpace;
  private final byte[] data;

  RemoteDirectiveRequest(List<String> directives, String pluginNameSpace, byte[] data) {
    this.directives = directives;
    this.pluginNameSpace = pluginNameSpace;
    this.data = data;
  }

  public byte[] getData() {
    return data;
  }

  public List<String> getDirectives() {
    return directives;
  }

  public String getPluginNameSpace() {
    return pluginNameSpace;
  }
}
