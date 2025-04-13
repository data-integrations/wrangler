/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.wrangler.api;

/**
 * Represents generic metadata information for a count metric that is emitted in Wrangler. The entity's type and name
 * will be automatically mapped to corresponding metric tags.
 */
public final class EntityCountMetric {
  /** The name of the metric. */
  private final String name;
  
  /** The count value to increment by. */
  private final long count;
  
  /** The type of system application entity. */
  private final String appEntityType;
  
  /** The name of the system application entity type. */
  private final String appEntityTypeName;

  /**
   * Creates a new entity count metric.
   *
   * @param name The name of the metric
   * @param appEntityType The type of system application entity
   * @param appEntityTypeName The name of the system application entity type
   * @param count The count value to increment by
   */
  public EntityCountMetric(String name, String appEntityType, String appEntityTypeName, long count) {
    this.name = name;
    this.appEntityType = appEntityType;
    this.appEntityTypeName = appEntityTypeName;
    this.count = count;
  }

  /**
   * Gets the name of the metric.
   *
   * @return The metric name
   */
  public String getName() {
    return name;
  }

  /**
   * Gets the type of system application entity.
   *
   * @return The entity type
   */
  public String getAppEntityType() {
    return appEntityType;
  }

  /**
   * Gets the name of the system application entity type.
   *
   * @return The entity type name
   */
  public String getAppEntityTypeName() {
    return appEntityTypeName;
  }

  /**
   * Gets the count value.
   *
   * @return The count value
   */
  public long getCount() {
    return count;
  }
}
