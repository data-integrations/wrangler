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

package io.cdap.wrangler.service.connections;

import io.cdap.cdap.api.Config;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Default connections to create at startup and connection types that needs to be disabled
 */
public class ConnectionTypeConfig extends Config {
  private final Set<ConnectionType> disabledTypes;
  private final List<Connection> connections;
  private final String defaultConnection;

  public ConnectionTypeConfig() {
    this(Collections.emptySet(), Collections.emptyList(), null);
  }

  public ConnectionTypeConfig(Set<ConnectionType> disabledTypes, List<Connection> connections,
                              @Nullable String defaultConnection) {
    this.disabledTypes = disabledTypes;
    this.connections = connections;
    this.defaultConnection = defaultConnection;
  }

  /**
   * Return the set of disabled connection types
   */
  public Set<ConnectionType> getDisabledTypes() {
    return disabledTypes == null ? Collections.emptySet() : disabledTypes;
  }

  /**
   * Return the list of default connections to be created
   */
  public List<Connection> getConnections() {
    return connections == null ? Collections.emptyList() : connections;
  }

  /**
   * Return the connection configured to be shown as default in dataprep - null if not provided
   */
  @Nullable
  public String getDefaultConnection() {
    return defaultConnection;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConnectionTypeConfig that = (ConnectionTypeConfig) o;

    return Objects.equals(disabledTypes, that.disabledTypes) &&
      Objects.equals(connections, that.connections) &&
      Objects.equals(defaultConnection, that.defaultConnection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(disabledTypes, connections, defaultConnection);
  }

}
