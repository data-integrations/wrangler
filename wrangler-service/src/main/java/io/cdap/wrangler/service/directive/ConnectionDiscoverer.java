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

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.etl.proto.connection.SampleResponseCodec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.NotFoundException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Discover for connection related operations
 */
public class ConnectionDiscoverer {
  private static final Gson GSON =
    new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(SampleResponse.class, new SampleResponseCodec()).create();

  private static final long TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
  private static final long RETRY_BASE_DELAY_MILLIS = 200L;
  private static final long RETRY_MAX_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(5);
  private static final double RETRY_DELAY_MULTIPLIER = 1.2d;
  private static final double RETRY_RANDOMIZE_FACTOR = 0.1d;

  private final ServiceDiscoverer serviceDiscoverer;

  public ConnectionDiscoverer(ServiceDiscoverer serviceDiscoverer) {
    this.serviceDiscoverer = serviceDiscoverer;
  }

  public SampleResponse retrieveSample(String namespace, String connectionName,
                                       SampleRequest sampleRequest) throws IOException {
    // Make call with exponential delay on failure retry.
    long delay = RETRY_BASE_DELAY_MILLIS;
    double minMultiplier = RETRY_DELAY_MULTIPLIER - RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    double maxMultiplier = RETRY_DELAY_MULTIPLIER + RETRY_DELAY_MULTIPLIER * RETRY_RANDOMIZE_FACTOR;
    Stopwatch stopWatch = Stopwatch.createStarted();
    try {
      while (stopWatch.elapsed(TimeUnit.MILLISECONDS) < TIMEOUT_MILLIS) {
        try {
          return executeSampleUrl(namespace, connectionName, sampleRequest);
        } catch (RetryableException e) {
          TimeUnit.MILLISECONDS.sleep(delay);
          delay = (long) (delay * (minMultiplier + Math.random() * (maxMultiplier - minMultiplier + 1)));
          delay = Math.min(delay, RETRY_MAX_DELAY_MILLIS);
        } catch (IOException e) {
          throw new IOException(String.format("Failed to retrieve sample for connection '%s' in namespace '%s'.",
                                              connectionName, namespace), e);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(String.format("Thread interrupted while trying to retrieve sample for " +
                                                 "connection '%s' in namespace '%s'.", connectionName, namespace), e);
    }
    throw new IllegalStateException(String.format("Timed out when trying to retrieve sample for " +
                                                    "connection '%s' in namespace '%s'.", connectionName, namespace));
  }

  private SampleResponse executeSampleUrl(String namespace, String connectionName,
                                          SampleRequest sampleRequest) throws IOException {
    HttpURLConnection urlConn = serviceDiscoverer.openConnection(
      NamespaceId.SYSTEM.getNamespace(), Constants.PIPELINEID,
      Constants.STUDIO_SERVICE_NAME, String.format("v1/contexts/%s/connections/%s/sample",
                                                   namespace, connectionName));

    if (urlConn == null) {
      throw new RetryableException("Connection service is not available");
    }

    urlConn.setRequestMethod("POST");
    try (OutputStream os = urlConn.getOutputStream();
         OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
      writer.write(GSON.toJson(sampleRequest));
      writer.flush();
    }

    int responseCode = urlConn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      switch (responseCode) {
        case HttpURLConnection.HTTP_BAD_GATEWAY:
        case HttpURLConnection.HTTP_UNAVAILABLE:
        case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
          throw new RetryableException("Connection service is not available with status " + responseCode);
        case HttpURLConnection.HTTP_BAD_REQUEST:
          throw new BadRequestException(getError(urlConn));
        case HttpURLConnection.HTTP_NOT_FOUND:
          throw new NotFoundException(getError(urlConn));
      }
      throw new IOException("Failed to call connection service with status " + responseCode + ": " +
                              getError(urlConn));
    }

    try (Reader reader = new InputStreamReader(urlConn.getInputStream(), StandardCharsets.UTF_8)) {
      return GSON.fromJson(CharStreams.toString(reader), SampleResponse.class);
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Returns the full content of the error stream for the given {@link HttpURLConnection}.
   */
  private String getError(HttpURLConnection urlConn) {
    try (InputStream is = urlConn.getErrorStream()) {
      if (is == null) {
        return "Unknown error";
      }
      return new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "Unknown error due to failure to read from error output: " + e.getMessage();
    }
  }
}
