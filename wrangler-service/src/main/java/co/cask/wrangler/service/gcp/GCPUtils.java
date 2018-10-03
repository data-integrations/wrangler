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
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
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
   * Create and return {@link Spanner} based on the credentials and project information provided in the connection
   */
  public static Spanner getSpannerService(Connection connection) throws Exception {
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder();
    if (connection.hasProperty(SERVICE_ACCOUNT_KEYFILE)) {
      String path = connection.getAllProps().get(SERVICE_ACCOUNT_KEYFILE);
      optionsBuilder.setCredentials(loadLocalFile(path));
    }

    String projectId = connection.hasProperty(PROJECT_ID) ?
      connection.getProp(PROJECT_ID) : ServiceOptions.getDefaultProjectId();
    if (projectId == null) {
      throw new IllegalArgumentException("Could not detect Google Cloud project id from the environment. " +
                                           "Please specify a project id for the connection.");
    }
    optionsBuilder.setProjectId(projectId);
    Spanner spanner = optionsBuilder.build().getService();
    return spanner;
  }

  /**
   * set credentials and project_id if those are provided in the input connection
   */
  private static void setProperties(Connection connection, ServiceOptions.Builder serviceOptions) throws Exception {
    if (connection.hasProperty(SERVICE_ACCOUNT_KEYFILE)) {
      String path = connection.getAllProps().get(SERVICE_ACCOUNT_KEYFILE);
      serviceOptions.setCredentials(loadLocalFile(path));
    }
    if (connection.hasProperty(PROJECT_ID)) {
      String projectId = connection.getAllProps().get(PROJECT_ID);
      serviceOptions.setProjectId(projectId);
    }
  }

  /**
   * Get the project id for the connection
   */
  public static String getProjectId(Connection connection) {
    String projectId = connection.getAllProps().get(GCPUtils.PROJECT_ID);
    return projectId == null ? ServiceOptions.getDefaultProjectId() : projectId;
  }

  /**
   * Validates that the project and credentials are either explicitly set in the connection or are available through
   * the environment.
   *
   * @param connection the connection to validate
   * @throws IllegalArgumentException if the project or credentials are not available
   */
  public static void validateProjectCredentials(Connection connection) {
    if (connection.getProp(PROJECT_ID) == null && ServiceOptions.getDefaultProjectId() == null) {
      throw new IllegalArgumentException("Project ID could not be found from the environment. " +
                                           "Please provide the Project ID.");
    }
    if (connection.getProp(SERVICE_ACCOUNT_KEYFILE) == null) {
      try {
        GoogleCredential.getApplicationDefault();
      } catch (IOException e) {
        throw new IllegalArgumentException("Google credentials could not be found from the environment. " +
                                             "Please provide a service account key.");
      }
    }
  }

  private GCPUtils() {
  }
}
