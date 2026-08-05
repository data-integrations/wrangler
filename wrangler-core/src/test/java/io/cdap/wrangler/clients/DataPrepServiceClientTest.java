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

import com.google.common.base.Throwables;
import io.cdap.cdap.etl.api.Arguments;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.wrangler.api.DirectiveConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DataPrepServiceClientTest {

  private static final String DEFAULT_RESPONSE =
      "{\"message\":\"Success\",\"count\":1,\"values\":[{}]}";

  private DataPrepServiceClient client;

  @Before
  public void setUp() {
    client = new DataPrepServiceClient();
  }

  private StageContext createMockStageContext(Map<String, String> argsMap, HttpURLConnection connection)
      throws IOException {
    Arguments arguments = Mockito.mock(Arguments.class);
    Mockito.when(arguments.get(Mockito.anyString())).thenAnswer(invocation -> {
      String key = invocation.getArgument(0);
      return argsMap.get(key);
    });

    StageContext context = Mockito.mock(StageContext.class);
    Mockito.when(context.getArguments()).thenReturn(arguments);
    Mockito.when(context.openConnection(
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenReturn(connection);
    return context;
  }

  private HttpURLConnection createMockConnection(int status, String responseBody) throws IOException {
    HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);
    Mockito.when(connection.getResponseCode()).thenReturn(status);
    if (responseBody != null) {
      Mockito.when(connection.getInputStream())
          .thenReturn(new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8)));
      Mockito.when(connection.getErrorStream())
          .thenReturn(new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8)));
    }
    return connection;
  }

  @Test
  public void testNullContextThrowsException() {
    try {
      client.fetchDirectiveConfig(null);
      Assert.fail("Expected IllegalArgumentException when context is null");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("StageContext cannot be null.", e.getMessage());
    }
  }

  @Test
  public void testNullOpenConnectionThrowsException() throws Exception {
    StageContext context = createMockStageContext(Collections.emptyMap(), null);
    try {
      client.fetchDirectiveConfig(context);
      Assert.fail("Expected exception when openConnection returns null");
    } catch (RuntimeException e) {
      Throwable rootCause = Throwables.getRootCause(e);
      Assert.assertTrue(rootCause instanceof NullPointerException);
      Assert.assertTrue(rootCause.getMessage().contains("openConnection returned null"));
    }
  }

  @Test
  public void testTimeoutConfiguration() throws Exception {
    // 1. Verify custom timeouts when configured arguments are provided
    Map<String, String> customArgs = new HashMap<>();
    customArgs.put(DataPrepServiceClient.CONNECT_TIMEOUT_PROP, "25000");
    customArgs.put(DataPrepServiceClient.READ_TIMEOUT_PROP, "90000");
    HttpURLConnection mockConnectionCustom =
        createMockConnection(HttpURLConnection.HTTP_OK, DEFAULT_RESPONSE);
    client.fetchDirectiveConfig(createMockStageContext(customArgs, mockConnectionCustom));
    Mockito.verify(mockConnectionCustom).setConnectTimeout(25000);
    Mockito.verify(mockConnectionCustom).setReadTimeout(90000);
    Mockito.verify(mockConnectionCustom).disconnect();

    // 2. Verify fallback to default timeouts (invalid connect timeout, empty read timeout)
    Map<String, String> fallbackArgs = Collections.singletonMap(
        DataPrepServiceClient.CONNECT_TIMEOUT_PROP, "invalid_number");
    HttpURLConnection mockConnectionFallback =
        createMockConnection(HttpURLConnection.HTTP_OK, DEFAULT_RESPONSE);
    client.fetchDirectiveConfig(createMockStageContext(fallbackArgs, mockConnectionFallback));
    Mockito.verify(mockConnectionFallback).setConnectTimeout(15000);
    Mockito.verify(mockConnectionFallback).setReadTimeout(60000);
    Mockito.verify(mockConnectionFallback).disconnect();
  }

  @Test
  public void testFetchDirectiveConfigParsesServiceResponse() throws Exception {
    String responseJson = "{\"message\":\"Success\",\"count\":1,\"values\":" +
      "[{\"exclusions\":[\"parse-as-json\",\"set-type\"],\"aliases\":{\"alias1\":\"actual1\"}}]," +
      "\"truncated\":\"false\"}";
    HttpURLConnection mockConnection = createMockConnection(HttpURLConnection.HTTP_OK, responseJson);
    StageContext context = createMockStageContext(Collections.emptyMap(), mockConnection);

    DirectiveConfig config = client.fetchDirectiveConfig(context);

    Assert.assertNotNull(config);
    Assert.assertTrue(config.isExcluded("parse-as-json"));
    Assert.assertTrue(config.isExcluded("set-type"));
    Assert.assertFalse(config.isExcluded("keep"));
    Assert.assertTrue(config.hasAlias("alias1"));
    Assert.assertEquals("actual1", config.getAliasName("alias1"));
    Mockito.verify(mockConnection).disconnect();
  }

  @Test
  public void testInternalErrorResponseThrowsRestClientExceptionWithoutRetry() throws Exception {
    String errorJson = "{\"message\":\"Internal Server Error\",\"status\":500}";
    HttpURLConnection mockConnection = createMockConnection(HttpURLConnection.HTTP_INTERNAL_ERROR, errorJson);
    StageContext context = createMockStageContext(Collections.emptyMap(), mockConnection);

    try {
      client.fetchDirectiveConfig(context);
      Assert.fail("Expected RestClientException when service returns HTTP 500");
    } catch (RuntimeException e) {
      Throwable rootCause = Throwables.getRootCause(e);
      Assert.assertTrue(rootCause instanceof RestClientException);
      RestClientException restException = (RestClientException) rootCause;
      Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, restException.getStatus());
      Assert.assertEquals(errorJson, restException.getMessage());
    }

    // RestClientException should NOT trigger retry; only 1 openConnection call expected
    Mockito.verify(context, Mockito.times(1)).openConnection(
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    Mockito.verify(mockConnection).disconnect();
  }

  @Test
  public void testErrorResponseWithoutErrorStream() throws Exception {
    HttpURLConnection mockConnection = Mockito.mock(HttpURLConnection.class);
    Mockito.when(mockConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_NOT_FOUND);
    Mockito.when(mockConnection.getErrorStream()).thenReturn(null);
    StageContext context = createMockStageContext(Collections.emptyMap(), mockConnection);

    try {
      client.fetchDirectiveConfig(context);
      Assert.fail("Expected RestClientException when service returns HTTP 404");
    } catch (RuntimeException e) {
      Throwable rootCause = Throwables.getRootCause(e);
      Assert.assertTrue(rootCause instanceof RestClientException);
      RestClientException restException = (RestClientException) rootCause;
      Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, restException.getStatus());
      Assert.assertEquals("", restException.getMessage());
    }
    Mockito.verify(mockConnection).disconnect();
  }

  @Test
  public void testRetryOnTransientIOExceptionSucceeds() throws Exception {
    HttpURLConnection mockConnection = createMockConnection(HttpURLConnection.HTTP_OK, DEFAULT_RESPONSE);

    Arguments arguments = Mockito.mock(Arguments.class);
    Mockito.when(arguments.get(Mockito.anyString())).thenReturn(null);

    StageContext context = Mockito.mock(StageContext.class);
    Mockito.when(context.getArguments()).thenReturn(arguments);
    Mockito.when(context.openConnection("system", "dataprep", "service", "config"))
        .thenThrow(new IOException("Transient connection failure"))
        .thenReturn(mockConnection);

    DirectiveConfig config = client.fetchDirectiveConfig(context);

    Assert.assertNotNull(config);
    Mockito.verify(context, Mockito.times(2)).openConnection(
        "system", "dataprep", "service", "config");
    Mockito.verify(mockConnection).disconnect();
  }

  @Test
  public void testConnectionHeadersAndMethod() throws Exception {
    HttpURLConnection mockConnection = createMockConnection(HttpURLConnection.HTTP_OK, DEFAULT_RESPONSE);
    StageContext context = createMockStageContext(Collections.emptyMap(), mockConnection);

    client.fetchDirectiveConfig(context);

    Mockito.verify(context).openConnection("system", "dataprep", "service", "config");
    Mockito.verify(mockConnection).setRequestMethod("GET");
    Mockito.verify(mockConnection).setRequestProperty("Accept", "application/json");
    Mockito.verify(mockConnection).disconnect();
  }
}
