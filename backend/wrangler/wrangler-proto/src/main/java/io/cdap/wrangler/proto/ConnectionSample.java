/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.wrangler.proto;

/**
 * Information about a sample taken using a connection.
 *
 * TODO: (CDAP-14652) check if these fields are all used/needed.
 * For example, why are 'connection' (actually connection type) and connection id in this object
 * when the client must supply this information in order to get the sample
 */
public class ConnectionSample {
  private final String id;
  private final String name;
  private final String connection;
  private final String sampler;
  private final String connectionid;

  public ConnectionSample(String id, String name, String connection, String sampler, String connectionid) {
    this.id = id;
    this.name = name;
    this.connection = connection;
    this.sampler = sampler;
    this.connectionid = connectionid;
  }
}
