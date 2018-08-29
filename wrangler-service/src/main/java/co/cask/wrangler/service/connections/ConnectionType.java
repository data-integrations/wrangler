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

package co.cask.wrangler.service.connections;

import co.cask.wrangler.api.annotations.PublicEvolving;

/**
 * This class {@link ConnectionType} defines different connections from which the data is extracted.
 */
@PublicEvolving
public enum ConnectionType {
  UNDEFINED("undefined"),
  UPLOAD("upload"),
  FILE("file"),
  DATABASE("database"),
  TABLE("table"),
  S3("s3"),
  GCS("gcs"),
  BIGQUERY("bigquery"),
  KAFKA("kafka"),
  SPANNER("spanner");

  private String type;

  ConnectionType(String type) {
    this.type = type;
  }

  /**
   * @return String representation of enum.
   */
  public String getType() {
    return type;
  }

  /**
   * Provided the connection type as string, determine the enum type of {@link ConnectionType}.
   *
   * @param from string for which the {@link ConnectionType} instance need to be determined.
   * @return if there is a string representation of enum, else null.
   */
  public static ConnectionType fromString(String from) {
    if (from == null || from.isEmpty()) {
      return null;
    }
    for (ConnectionType method : ConnectionType.values()) {
      if (method.type.equalsIgnoreCase(from)) {
        return method;
      }
    }
    return null;
  }
}
