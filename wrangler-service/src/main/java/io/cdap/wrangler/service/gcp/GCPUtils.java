/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.wrangler.service.gcp;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.connection.ConnectionMeta;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Class description here.
 */
public final class GCPUtils {
  public static final String PROJECT_ID = "projectId";
  public static final String SERVICE_ACCOUNT_KEYFILE = "service-account-keyfile";

  public static ServiceAccountCredentials loadLocalFile(String path) throws IOException {
    return loadLocalFile(path, Collections.emptyList());
  }

  public static ServiceAccountCredentials loadLocalFile(String path, List<String> scopes) throws IOException {
    File credentialsPath = new File(path);
    if (!credentialsPath.exists()) {
      throw new FileNotFoundException("Service account file " + credentialsPath.getName() + " does not exist.");
    }
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      ServiceAccountCredentials serviceAccountCredentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
      return (ServiceAccountCredentials) serviceAccountCredentials.createScoped(scopes);
    } catch (FileNotFoundException e) {
      throw new IOException(
        String.format("Unable to find service account file '%s'.", path), e);
    } catch (IOException e) {
      throw new IOException(
        String.format("Issue reading service account file '%s', please check permission of the file", path), e);
    }
  }

  /**
   * Create and return {@link Storage} based on the credentials and project information provided in the connection
   */
  public static Storage getStorageService(ConnectionMeta connection) throws IOException {
    StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
    setProperties(connection, storageOptionsBuilder, Collections.emptyList());
    return storageOptionsBuilder.build().getService();
  }

  /**
   * Create and return {@link BigQuery} based on the credentials and project information provided in the connection
   */
  public static BigQuery getBigQueryService(ConnectionMeta connection) throws IOException {
    BigQueryOptions.Builder bigQueryOptionsBuilder = BigQueryOptions.newBuilder();
    setProperties(connection, bigQueryOptionsBuilder, Arrays.asList("https://www.googleapis.com/auth/drive",
                                                                    "https://www.googleapis.com/auth/bigquery"));
    return bigQueryOptionsBuilder.build().getService();
  }

  /**
   * Create and return {@link Spanner} based on the credentials and project information provided in the connection
   */
  public static Spanner getSpannerService(ConnectionMeta connection) throws IOException {
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder();
    Map<String, String> connProperties = connection.getProperties();
    if (connProperties.containsKey(SERVICE_ACCOUNT_KEYFILE)) {
      String path = connProperties.get(SERVICE_ACCOUNT_KEYFILE);
      optionsBuilder.setCredentials(loadLocalFile(path));
    }

    String projectId = connProperties.containsKey(PROJECT_ID) ?
      connProperties.get(PROJECT_ID) : ServiceOptions.getDefaultProjectId();
    if (projectId == null) {
      throw new BadRequestException("Could not detect Google Cloud project id from the environment. " +
                                      "Please specify a project id for the connection.");
    }
    optionsBuilder.setProjectId(projectId);
    Spanner spanner = optionsBuilder.build().getService();
    return spanner;
  }

  /**
   * set credentials and project_id if those are provided in the input connection
   */
  private static void setProperties(ConnectionMeta connection,
                                    ServiceOptions.Builder serviceOptions, List<String> scopes) throws IOException {
    if (connection.getProperties().containsKey(SERVICE_ACCOUNT_KEYFILE)) {
      String path = connection.getProperties().get(SERVICE_ACCOUNT_KEYFILE);
      serviceOptions.setCredentials(loadLocalFile(path, scopes));
    }
    if (connection.getProperties().containsKey(PROJECT_ID)) {
      String projectId = connection.getProperties().get(PROJECT_ID);
      serviceOptions.setProjectId(projectId);
    }
  }

  /**
   * Get the project id for the connection
   */
  public static String getProjectId(ConnectionMeta connection) {
    String projectId = connection.getProperties().get(GCPUtils.PROJECT_ID);
    return projectId == null ? ServiceOptions.getDefaultProjectId() : projectId;
  }

  /**
   * Validates that the project and credentials are either explicitly set in the connection or are available through
   * the environment.
   *
   * @param connection the connection to validate
   * @throws IllegalArgumentException if the project or credentials are not available
   */
  public static void validateProjectCredentials(ConnectionMeta connection) {
    Map<String, String> connProperties = connection.getProperties();
    if (connection.getProperties().get(PROJECT_ID) == null && ServiceOptions.getDefaultProjectId() == null) {
      throw new BadRequestException("Project ID could not be found from the environment. " +
                                      "Please provide the Project ID.");
    }
    if (connProperties.get(SERVICE_ACCOUNT_KEYFILE) == null) {
      try {
        GoogleCredential.getApplicationDefault();
      } catch (IOException e) {
        throw new BadRequestException("Google credentials could not be found from the environment. " +
                                        "Please provide a service account key.");
      }
    }
  }

  private GCPUtils() {
  }
}
