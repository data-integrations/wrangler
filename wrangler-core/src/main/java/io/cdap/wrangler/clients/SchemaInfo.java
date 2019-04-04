/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.clients;

import java.util.Set;

/**
 * This class is POJO that provides information about the schema being requested.
 */
final class SchemaInfo {
  // Id of the schema being stored in the schema repository.
  private String id;

  // Each id of the schema is associated with name.
  private String name;

  // Each schema can have multiple versions, so this will show the version that is being retrieved.
  private long version;

  // Schema description.
  private String description;

  // Type of the schema.
  private String type;

  // Current active version of schema.
  private long current;

  // Specification of the schema.
  private String specification;

  // List of available versions of schema.
  private Set<Long> versions;

  /**
   * @return id of the schema being requested.
   */
  public String getId() {
    return id;
  }

  /**
   * @return name of the schema.
   */
  public String getName() {
    return name;
  }

  /**
   * @return version of schema being requested.
   */
  public long getVersion() {
    return version;
  }

  /**
   * @return description of schema being requested.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return string representation of schema being requested.
   */
  public String getType() {
    return type;
  }

  /**
   * @return the current version of schema that is active.
   */
  public long getCurrent() {
    return current;
  }

  /**
   * @return schema definition being requested.
   */
  public String getSpecification() {
    return specification;
  }

  /**
   * @return List of available versions of schema.
   */
  public Set<Long> getVersions() {
    return versions;
  }

}
