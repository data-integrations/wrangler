/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.wrangler.service.gcp;

import co.cask.wrangler.dataset.connections.Connection;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Class description here.
 */
public final class GCPUtils {

  public static final String PROJECT_ID = "projectId";
  public static final String SERVICE_ACCOUNT_KEYFILE = "service-account-keyfile";

  public static ServiceAccountCredentials loadLocalFile(String path) throws Exception {
    File credentialsPath = new File(path);
    if (!credentialsPath.exists()) {
      throw new FileNotFoundException("Service account file " + credentialsPath.getName() + " does not exist.");
    }
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    } catch (FileNotFoundException e) {
      throw new Exception(
        String.format("Unable to find service account file '%s'.", path)
      );
    } catch (IOException e) {
      throw new Exception(
        String.format(
          "Issue reading service account file '%s', please check permission of the file", path
        )
      );
    }
  }

  /**
   * Create and return {@link Storage} based on the credentials and project information provided in the connection
   */
  public static Storage getStorageService(Connection connection) throws Exception {
    StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
    setProperties(connection, storageOptionsBuilder);
    return storageOptionsBuilder.build().getService();
  }

  /**
   * Create and return {@link BigQuery} based on the credentials and project information provided in the connection
   */
  public static BigQuery getBigQueryService(Connection connection) throws Exception {
    BigQueryOptions.Builder bigQueryOptionsBuilder = BigQueryOptions.newBuilder();
    setProperties(connection, bigQueryOptionsBuilder);
    return bigQueryOptionsBuilder.build().getService();
  }

  /**
   * set credentials and project_id if those are provided in the input connection
   */
  private static void setProperties(Connection connection, ServiceOptions.Builder serviceOptions) throws Exception {
    if (connection.hasProperty(GCPUtils.SERVICE_ACCOUNT_KEYFILE)) {
      String path = connection.getAllProps().get(GCPUtils.SERVICE_ACCOUNT_KEYFILE);
      serviceOptions.setCredentials(loadLocalFile(path));
    }
    if (connection.hasProperty(GCPUtils.PROJECT_ID)) {
      String projectId = connection.getAllProps().get(GCPUtils.PROJECT_ID);
      serviceOptions.setProjectId(projectId);
    }
  }

  private GCPUtils() {
  }
}
