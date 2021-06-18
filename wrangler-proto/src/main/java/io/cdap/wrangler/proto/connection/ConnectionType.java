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


import java.util.EnumSet;
import javax.annotation.Nullable;

/**
 * This class {@link ConnectionType} defines different connections from which the data is extracted.
 *
 * TODO: (CDAP-14619) make casing consistent. User requests currently must have the type as upper case,
 *  then the lower case version is stored in several places and returned in several responses.
 */
public enum ConnectionType {
  UPLOAD("upload"),
  FILE("file"),
  DATABASE("database", "Database"),
  TABLE("table"),
  S3("s3", "S3"),
  GCS("gcs", "GCS"),
  ADLS("adls"),
  BIGQUERY("bigquery", "BigQuery"),
  KAFKA("kafka", "Kafka"),
  SPANNER("spanner", "Spanner");

  // file won't be upgraded since file connection type is meant for local file system and not packaged in distributed
  // cdap
  public static final EnumSet CONN_UPGRADABLE_TYPES = EnumSet.of(DATABASE, S3, GCS, BIGQUERY, KAFKA, SPANNER);
  // upload is upgradable and it will get translates to a no-source pipeline in studio
  public static final EnumSet WORKSPACE_UPGRADABLE_TYPES =
    EnumSet.of(DATABASE, S3, GCS, BIGQUERY, KAFKA, SPANNER, UPLOAD);

  private final String type;
  private final String connectorName;

  ConnectionType(String type) {
    this(type, null);
  }

  ConnectionType(String type, @Nullable String connectorName) {
    this.type = type;
    this.connectorName = connectorName;
  }

  /**
   * @return String representation of enum.
   */
  public String getType() {
    return type;
  }

  @Nullable
  public String getConnectorName() {
    return connectorName;
  }
}
