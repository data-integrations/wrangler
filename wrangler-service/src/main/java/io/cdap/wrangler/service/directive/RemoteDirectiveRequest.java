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

import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.dataset.workspace.DataType;

/**
 * RemoteDirectiveRequest
 */
public class RemoteDirectiveRequest {

  private String userRequestString;
  private String pluginNameSpace;
  private String id;
  private DataType datatype;
  private byte[] data;
  private DirectiveConfig config;

  RemoteDirectiveRequest(String userRequestString, String pluginNameSpace, String id) {
    this.userRequestString = userRequestString;
    this.pluginNameSpace = pluginNameSpace;
    this.id = id;
  }

  public DataType getDatatype() {
    return datatype;
  }

  public byte[] getData() {
    return data;
  }

  public void setConfig(DirectiveConfig config) {
    this.config = config;
  }

  public String getUserRequestString() {
    return userRequestString;
  }

  public String getPluginNameSpace() {
    return pluginNameSpace;
  }

  public String getId() {
    return id;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public void setDatatype(DataType datatype) {
    this.datatype = datatype;
  }

  public DirectiveConfig getConfig() {
    return config;
  }
}
