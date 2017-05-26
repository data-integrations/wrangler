/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.dataset.connections;

import co.cask.wrangler.service.connections.ConnectionType;

import java.util.HashMap;
import java.util.Map;

/**
 * Basic connection object.
 */
public final class Connection {
  // Id of the connection.
  private String id;

  // Name of the connection.
  private String name;

  // Type of the connection.
  private ConnectionType type;

  // Description for the connection.
  private String description;

  // Time in seconds - when it was created.
  private long created;

  // Time in second - when it was last updated.
  private long updated;

  // Collection of properties.
  private Map<String, Object> properties = new HashMap<>();

  /**
   * @return id of the connection.
   */
  public String getId() {
    return id;
  }

  /**
   * @return name of the connection.
   */
  public String getName() {
    return name;
  }

  /**
   * @return type of the connection.
   */
  public ConnectionType getType() {
    return type;
  }

  /**
   * @return description of the connection.
   */
  public String getDescription() {
    return description;
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

  /**
   * Adds a property to the connection.
   *
   * @param key to be added.
   * @param value to be added.
   */
  public void putProp(String key, Object value) {
    properties.put(key, value);
  }

  /**
   * Retrieves a property from the connection.
   *
   * @param key to be retrieved.
   * @return the value for the key, else null.
   */
  public <T> T getProp(String key) {
    return (T) properties.get(key);
  }

  /**
   * Checks if connection has a property defined or not.
   *
   * @param key to be checked for presence.
   * @return true if present, else false.
   */
  public boolean hasProperty(String key) {
    return properties.containsKey(key);
  }

  /**
   * Sets the id for the connection.
   *
   * @param id to be set.
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Sets the name for the connection.
   *
   * @param name of the connection.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Sets the type of the connection.
   *
   * @param type of the connection to be set.
   */
  public void setType(ConnectionType type) {
    this.type = type;
  }

  /**
   * Sets the description for the connection.
   *
   * @param description for the connection.
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Sets the created date in seconds for the connection.
   *
   * @param created date for the connection.
   */
  public void setCreated(long created) {
    this.created = created;
  }

  /**
   * Sets the last updated date time in second for the connection.
   *
   * @param updated date time for the connection.
   */
  public void setUpdated(long updated) {
    this.updated = updated;
  }

  /**
   * @return all the properties associated with the connection.
   */
  public Map<String, Object> getAllProps() {
    return properties;
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
}
