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
public class EntityCountMetric {
  /**
   * Metric name
   */
  private final String name;
  /**
   * Value by which to increment the count
   */
  private final long count;
  /**
   * System app entity type
   */
  private final String appEntityType;
  /**
   * System app entity type name
   */
  private final String appEntityTypeName;

  public EntityCountMetric(String name, String appEntityType, String appEntityTypeName, long count) {
    this.name = name;
    this.appEntityType = appEntityType;
    this.appEntityTypeName = appEntityTypeName;
    this.count = count;
  }

  public String getName() {
    return name;
  }

  public String getAppEntityType() {
    return appEntityType;
  }

  public String getAppEntityTypeName() {
    return appEntityTypeName;
  }

  public long getCount() {
    return count;
  }
}
