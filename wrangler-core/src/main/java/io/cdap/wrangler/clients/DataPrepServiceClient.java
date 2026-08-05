/*
 * Copyright © 2026 Cask Data, Inc.
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

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.proto.ServiceResponse;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Client for interacting with the system-level DataPrep service.
 * Supports configurable connection/read timeouts via pipeline runtime arguments
 * and implements exponential backoff retry logic for resilience against
 * transient network failures.
 */
public class DataPrepServiceClient {
  private static final Logger LOG = LoggerFactory.getLogger(DataPrepServiceClient.class);
  private static final Gson GSON = new Gson();

  private static final String SYSTEM_NAMESPACE = "system";
  private static final String APPLICATION_NAME = "dataprep";
  private static final String SERVICE_NAME = "service";
  private static final String CONFIG_ENDPOINT = "config";

  public static final String CONNECT_TIMEOUT_PROP = "wrangler.service.connect.timeout.ms";
  public static final String READ_TIMEOUT_PROP = "wrangler.service.read.timeout.ms";

  // Matches cdap-default.xml http.client.connection.timeout.ms and
  // http.client.read.timeout.ms
  private static final int DEFAULT_CONNECT_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(15);
  private static final int DEFAULT_READ_TIMEOUT_MS = (int) TimeUnit.SECONDS.toMillis(60);

  private static final Retryer<DirectiveConfig> DIRECTIVE_CONFIG_RETRYER = RetryerBuilder.<DirectiveConfig>newBuilder()
      .retryIfExceptionOfType(IOException.class)
      .withWaitStrategy(WaitStrategies.exponentialWait(1, 10, TimeUnit.SECONDS))
      .withStopStrategy(StopStrategies.stopAfterDelay(5, TimeUnit.MINUTES))
      .withRetryListener(new RetryListener() {
        @Override
        public <V> void onRetry(Attempt<V> attempt) {
          if (attempt.hasException()) {
            Throwable cause = attempt.getExceptionCause();
            if (cause != null) {
              LOG.warn("Attempt {} to fetch directive config from DataPrep service failed: {}. Retrying...",
                  attempt.getAttemptNumber(), cause.getMessage());
            }
          }
        }
      })
      .build();

  /**
   * Fetches directive config from the system-level DataPrep service with
   * exponential backoff retry.
   *
   * @param context the stage context
   * @return the fetched DirectiveConfig
   */
  public DirectiveConfig fetchDirectiveConfig(StageContext context) {
    try {
      Preconditions.checkArgument(context != null, "StageContext cannot be null.");
      return DIRECTIVE_CONFIG_RETRYER.call(() -> getDirectiveConfig(context));
    } catch (ExecutionException | RetryException e) {
      throw Throwables.propagate(e);
    }
  }

  private DirectiveConfig getDirectiveConfig(StageContext context) throws IOException, RestClientException {
    HttpURLConnection connection = null;
    try {
      LOG.debug("Fetching directive config from DataPrep service.");
      connection = createConnection(context);
      int responseCode = connection.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        handleErrorResponse(connection, responseCode);
      }

      try (InputStream is = connection.getInputStream()) {
        String responseBody = IOUtils.toString(is, StandardCharsets.UTF_8);
        LOG.debug("Received HTTP response from GET /config: {}", responseBody);
        if (Strings.isNullOrEmpty(responseBody)) {
          throw new IOException("Received null or empty response body from DataPrep service.");
        }
        ServiceResponse<DirectiveConfig> response = GSON.fromJson(responseBody,
            new TypeToken<ServiceResponse<DirectiveConfig>>() {
            }.getType());
        if (response.getValues() != null && !response.getValues().isEmpty()) {
          DirectiveConfig config = response.getValues().iterator().next();
          return config;
        }
        throw new IOException("Received null or malformed directive configuration from DataPrep service.");
      }
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private HttpURLConnection createConnection(StageContext context) throws IOException {
    HttpURLConnection connection = Objects.requireNonNull(
        context.openConnection(SYSTEM_NAMESPACE, APPLICATION_NAME, SERVICE_NAME, CONFIG_ENDPOINT),
        "Failed to establish connection to DataPrep service: openConnection returned null.");
    connection.setRequestMethod("GET");
    int connectTimeout = tryParseTimeout(context, CONNECT_TIMEOUT_PROP, DEFAULT_CONNECT_TIMEOUT_MS);
    int readTimeout = tryParseTimeout(context, READ_TIMEOUT_PROP, DEFAULT_READ_TIMEOUT_MS);
    connection.setConnectTimeout(connectTimeout);
    connection.setReadTimeout(readTimeout);
    connection.setRequestProperty("Accept", "application/json");
    return connection;
  }

  private void handleErrorResponse(HttpURLConnection connection, int responseCode)
      throws IOException, RestClientException {
    LOG.debug("HTTP GET /config response code: {}", responseCode);
    String errorMessage = "";
    try (InputStream es = connection.getErrorStream()) {
      if (es != null) {
        errorMessage = IOUtils.toString(es, StandardCharsets.UTF_8);
      }
    }
    throw new RestClientException(responseCode, errorMessage);
  }

  private int tryParseTimeout(StageContext context, String propKey, int defaultTimeoutMs) {
    String value = context.getArguments().get(propKey);
    if (Strings.isNullOrEmpty(value)) {
      return defaultTimeoutMs;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      LOG.warn("Invalid runtime argument for {}: '{}'. Using default timeout {} ms.",
          propKey, value, defaultTimeoutMs, e);
      return defaultTimeoutMs;
    }
  }
}
