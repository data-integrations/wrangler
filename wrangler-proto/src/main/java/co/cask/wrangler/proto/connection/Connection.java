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

package co.cask.wrangler.proto.connection;

import java.util.Map;
import java.util.Objects;

/**
 * Basic connection object.
 */
public final class Connection extends ConnectionMeta {
  // Id of the connection.
  private String id;

  // Time in seconds - when it was created.
  private long created;

  // Time in second - when it was last updated.
  private long updated;

  public Connection(String id, ConnectionType type, String name, String description, long created, long updated,
                    Map<String, String> properties) {
    super(type, name, description, properties);
    this.id = id;
    this.created = created;
    this.updated = updated;
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
      Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), id, created, updated);
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
      '}';
  }

  public static Builder builder(String id) {
    return new Builder(id);
  }

  public static Builder builder(String id, ConnectionMeta meta) {
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
    private final String id;
    private long created = -1L;
    private long updated = -1L;

    public Builder(String id) {
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

    public Connection build() {
      if (created < 0) {
        throw new IllegalStateException("Created time must be above 0.");
      }
      if (updated < 0) {
        throw new IllegalStateException("Updated time must be above 0.");
      }
      return new Connection(id, type, name, description, created, updated, properties);
    }
  }
}
