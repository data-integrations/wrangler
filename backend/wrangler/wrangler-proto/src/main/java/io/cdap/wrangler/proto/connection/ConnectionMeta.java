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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata about a connection.
 */
public class ConnectionMeta {
  protected final ConnectionType type;
  protected final String name;
  protected final String description;
  protected final Map<String, String> properties;

  protected ConnectionMeta(ConnectionType type, String name, String description,
                           Map<String, String> properties) {
    this.type = type;
    this.name = name;
    this.description = description;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  public ConnectionType getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description == null ? "" : description;
  }

  public Map<String, String> getProperties() {
    return properties == null ? Collections.emptyMap() : properties;
  }

  /**
   * Should be called if this object is created through deserialization of user input.
   *
   * @throws IllegalArgumentException if the object is invalid.
   */
  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("A connection name must be specified.");
    }
    if (type == null) {
      throw new IllegalArgumentException("A connection type must be specified.");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConnectionMeta that = (ConnectionMeta) o;
    return type == that.type &&
      Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, description, properties);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static <T extends ConnectionMeta> Builder builder(T existing) {
    return new Builder().setName(existing.getName())
      .setType(existing.getType())
      .setDescription(existing.getDescription())
      .setProperties(existing.getProperties());
  }

  /**
   * Creates ConnectionMeta objects.
   *
   * @param <T> type of builder
   */
  public static class Builder<T extends Builder> {
    protected ConnectionType type;
    protected String name;
    protected String description;
    protected Map<String, String> properties = new HashMap<>();

    public T setType(ConnectionType type) {
      this.type = type;
      return (T) this;
    }

    public T setName(String name) {
      this.name = name;
      return (T) this;
    }

    public T setDescription(String description) {
      this.description = description;
      return (T) this;
    }

    public T setProperties(Map<String, String> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return (T) this;
    }

    public T putProperty(String key, String val) {
      this.properties.put(key, val);
      return (T) this;
    }

    public ConnectionMeta build() {
      ConnectionMeta meta = new ConnectionMeta(type, name, description, properties);
      meta.validate();
      return meta;
    }
  }
}
