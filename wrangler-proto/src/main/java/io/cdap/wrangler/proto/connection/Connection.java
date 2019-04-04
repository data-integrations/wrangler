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

package io.cdap.wrangler.proto.connection;

import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.proto.NamespacedId;

import java.util.Map;
import java.util.Objects;

/**
 * Basic connection object.
 */
public final class Connection extends ConnectionMeta {
  private final String context;
  private final String id;
  // transient so it doesn't show up in REST endpoint responses
  private final transient NamespacedId namespacedId;

  // Time in seconds - when it was created.
  private final long created;

  // Time in second - when it was last updated.
  private final long updated;

  // Determines if the connection is configured automatically
  private final boolean preconfigured;

  public Connection(NamespacedId id, ConnectionType type, String name, String description, long created, long updated,
                    Map<String, String> properties) {
    this(id, type, name, description, created, updated, properties, false);
  }

  public Connection(NamespacedId id, ConnectionType type, String name, String description, long created, long updated,
                    Map<String, String> properties, boolean preconfigured) {
    super(type, name, description, properties);
    this.namespacedId = id;
    this.context = id.getNamespace().getName();
    this.id = id.getId();
    this.created = created;
    this.updated = updated;
    this.preconfigured = preconfigured;
  }

  public String getNamespace() {
    return context == null ? Contexts.DEFAULT : context;
  }

  /**
   * @return id of the connection.
   */
  public String getId() {
    return id;
  }

  /**
   * @return time in second when the connection was created.
   */
  public long getCreated() {
    return created;
  }

  /**
   * @return time in second when the connection was last updated.
   */
  public long getUpdated() {
    return updated;
  }

  public boolean isPreconfigured() {
    return preconfigured;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    Connection that = (Connection) o;
    return created == that.created &&
      updated == that.updated &&
      preconfigured == that.preconfigured &&
      Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), id, created, updated, preconfigured);
  }

  /**
   * @return string representation of object.
   */
  @Override
  public String toString() {
    return "Connection{" +
      "id='" + id + '\'' +
      ", name='" + name + '\'' +
      ", type=" + type +
      ", description='" + description + '\'' +
      ", created=" + created +
      ", updated=" + updated +
      ", properties=" + properties +
      ", preconfigured=" + preconfigured +
      '}';
  }

  public static Builder builder(NamespacedId id) {
    return new Builder(id);
  }

  public static Builder builder(NamespacedId id, ConnectionMeta meta) {
    return new Builder(id)
      .setName(meta.getName())
      .setType(meta.getType())
      .setDescription(meta.getDescription())
      .setProperties(meta.getProperties());
  }

  /**
   * Creates Connections.
   */
  public static class Builder extends ConnectionMeta.Builder<Builder> {
    private final NamespacedId id;
    private long created = -1L;
    private long updated = -1L;
    private boolean preconfigured = false;

    public Builder(NamespacedId id) {
      this.id = id;
    }

    public Builder setCreated(long created) {
      this.created = created;
      return this;
    }

    public Builder setUpdated(long updated) {
      this.updated = updated;
      return this;
    }

    public Builder setPreconfigured(boolean preconfigured) {
      this.preconfigured = preconfigured;
      return this;
    }

    public Connection build() {
      if (created < 0) {
        throw new IllegalStateException("Created time must be above 0.");
      }
      if (updated < 0) {
        throw new IllegalStateException("Updated time must be above 0.");
      }
      return new Connection(id, type, name, description, created, updated, properties, preconfigured);
    }
  }
}
