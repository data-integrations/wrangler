/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler.service.directive;

import io.cdap.wrangler.dataset.workspace.Workspace;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.service.bigquery.BigQueryHandler;
import io.cdap.wrangler.service.database.DatabaseHandler;
import io.cdap.wrangler.service.gcs.GCSHandler;
import io.cdap.wrangler.service.kafka.KafkaHandler;
import io.cdap.wrangler.service.s3.S3Handler;
import io.cdap.wrangler.service.spanner.SpannerHandler;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Specification util specifically needed for upgrade. Can be used to generate the spec that is known by the
 * connector/source or generate path. This class is mostly hardcoding and should be removed once we remove the
 * deprecated handlers.
 */
public class SpecificationUpgradeUtils {

  private SpecificationUpgradeUtils() {
  }

  /**
   * Method to retrieve the spec that is known to connector/source, this is needed because the connection type handlers
   * sometimes use a different config name for property, need to have this method to generate the connector properties
   * in order for the new connection to use.
   *
   * @param connectionType the connection type to check
   * @param properties properties to retrieve connector/source properties
   * @return properties that can be used by connector
   */
  public static Map<String, String> getConnectorProperties(ConnectionType connectionType,
                                                           Map<String, String> properties) {
    if (!ConnectionType.CONN_UPGRADABLE_TYPES.contains(connectionType)) {
      return properties;
    }

    switch (connectionType) {
      case BIGQUERY:
        return BigQueryHandler.getConnectorProperties(properties);
      case GCS:
        return GCSHandler.getConnectorProperties(properties);
      case SPANNER:
        return SpannerHandler.getConnectorProperties(properties);
      case S3:
        return S3Handler.getConnectorProperties(properties);
      case DATABASE:
        return DatabaseHandler.getConnectorProperties(properties);
      case KAFKA:
        return KafkaHandler.getConnectorProperties(properties);
      default:
        return properties;
    }
  }

  /**
   * Get a connector path from the v1 workspace
   *
   * @param connectionType connectionType of the workspace
   * @param workspace the v1 workspace information
   * @return a connector path, null if the connection type cannot be upgraded to a new connection
   */
  @Nullable
  public static String getPath(ConnectionType connectionType, Workspace workspace) {
    if (!ConnectionType.CONN_UPGRADABLE_TYPES.contains(connectionType)) {
      return null;
    }

    switch (connectionType) {
      case BIGQUERY:
        return BigQueryHandler.getPath(workspace);
      case GCS:
        return GCSHandler.getPath(workspace);
      case SPANNER:
        return SpannerHandler.getPath(workspace);
      case S3:
        return S3Handler.getPath(workspace);
      case DATABASE:
        return DatabaseHandler.getPath(workspace);
      case KAFKA:
        return KafkaHandler.getPath(workspace);
      default:
        return null;
    }
  }
}
