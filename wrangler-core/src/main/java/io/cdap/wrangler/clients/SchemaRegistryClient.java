/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.wrangler.api.ExecutorContext;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class {@link SchemaRegistryClient} is a client API for the SchemaRegistry service.
 *
 * Client allows only read-only access to service. It doesn't support the ability to mutate
 * the service.
 *
 * Example usage of the client.
 * <code>
 *   ...
 *   SchemaRegistryClient client = new SchemaRegistryClient(<baseurl>);
 *   ...
 *   client.setAcceptEncoding("application/json");
 *   client.setConnectionTimeout(2000); // 2 seconds of connect timeout.
 *   client.setReadTimeout(1000); // 1 second of read timeout.
 *   try {
 *    Response<SchemaInfo> info = client.getSchema("test", 2);
 *   } catch (IOException e) {
 *     ...
 *   } catch (URIException e) {
 *     ...
 *   } catch (RestClientException e) {
 *     ...
 *   }
 *   ...
 * </code>
 */
public final class SchemaRegistryClient {
  private final Gson gson;

  // Defines the service base url.
  private final String baseUrl;

  // Connections settings.
  private int connectionTimeout; // Timeout to connect specified in milliseconds.
  private int readTimeout; // Timeout to read from socket in milliseconds.
  private String acceptEncoding; // Accepting content type.

  public SchemaRegistryClient(String baseUrl) {
    this.baseUrl = baseUrl;
    this.connectionTimeout = 2000;
    this.readTimeout = 1000;
    this.acceptEncoding = "application/json";
    this.gson = new GsonBuilder().create();
  }

  /**
   * Returns a instance of {@link SchemaRegistryClient} initialized with service url.
   *
   * @param context of the pipeline in which this client is invoked.
   * @return instance of this class.
   */
  public static SchemaRegistryClient getInstance(ExecutorContext context) {
    return new SchemaRegistryClient(context.getService("dataprep", "service").toString());
  }

  /**
   * Retrieves schema provided a schema id and version of the schema.
   *
   * @param namespace the schema namespace
   * @param id the schema id
   * @param version the schema version
   * @return {@link Response} of the schema.
   * @throws URISyntaxException thrown if there are issue with construction of url.
   * @throws IOException throw when there are issues connecting to the service.
   * @throws RestClientException thrown when there are issues with request or response returned.
   */
  public byte[] getSchema(String namespace, String id, long version)
    throws URISyntaxException, IOException, RestClientException {
    URL url = concat(new URI(baseUrl),
                     String.format("contexts/%s/schemas/%s/versions/%d", namespace, id, version)).toURL();
    Response<SchemaInfo> response = request(url, "GET", new TypeToken<Response<SchemaInfo>>() { }.getType());
    if (response.getCount() == 1) {
      return Bytes.fromHexString(response.getValues().get(0).getSpecification());
    }
    return null;
  }

  /**
   * Retrieves schema provided a schema id. It provides the latest, current version of schema.
   *
   * @param namespace the schema namespace
   * @param id the schema id
   * @return {@link SchemaInfo} of the schema if ok, else null.
   * @throws URISyntaxException thrown if there are issue with construction of url.
   * @throws IOException throw when there are issues connecting to the service.
   * @throws RestClientException thrown when there are issues with request or response returned.
   */
  public byte[] getSchema(String namespace, String id)
    throws URISyntaxException, IOException, RestClientException {
    URL url = concat(new URI(baseUrl), String.format("contexts/%s/schemas/%s", namespace, id)).toURL();
    Response<SchemaInfo> response = request(url, "GET", new TypeToken<Response<SchemaInfo>>() { }.getType());
    if (response.getCount() == 1) {
      return Bytes.fromHexString(response.getValues().get(0).getSpecification());
    }
    return null;
  }

  /**
   * Gets all the versions of schemas given a schema id.
   *
   * @param namespace the schema namespace
   * @param id the schema id
   * @return a list of schema versions.
   * @throws URISyntaxException thrown if there are issue with construction of url.
   * @throws IOException throw when there are issues connecting to the service.
   * @throws RestClientException thrown when there are issues with request or response returned.
   */
  public List<Long> getVersions(String namespace, String id)
    throws URISyntaxException, IOException, RestClientException {
    URL url = concat(new URI(baseUrl),
                     String.format("contexts/%s/schemas/%s/versions", namespace, id)).toURL();
    Response<Long> response = request(url, "GET", new TypeToken<Response<Long>>() { }.getType());
    return response.getValues();
  }

  /**
   * @return the base url set for this client.
   */
  public String getBaseUrl() {
    return baseUrl;
  }

  /**
   * @return Accept encoding currently setup for the client.
   */
  public String getAcceptEncoding() {
    return acceptEncoding;
  }

  /**
   * @param acceptEncoding sets the accept encoding for the client.
   */
  public void setAcceptEncoding(String acceptEncoding) {
    this.acceptEncoding = acceptEncoding;
  }

  /**
   * @return the timeout set for connection in milliseconds.
   */
  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  /**
   * Sets the connection timeout in milliseconds.
   * @param connectionTimeout specified in milliseconds.
   */
  public void setConnectionTimeout(int connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  /**
   * @return the read timeout in milliseconds.
   */
  public int getReadTimeout() {
    return readTimeout;
  }

  /**
   * Sets the read timeout in milliseconds.
   * @param readTimeout
   */
  public void setReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
  }

  private URI concat(URI uri, String extraPath)
    throws URISyntaxException, MalformedURLException {
    String newPath = uri.getPath() + '/' + extraPath;
    URI newUri = uri.resolve(newPath);
    return newUri;
  }

  private <T> T request(URL url, String method, Type classz) throws IOException, RestClientException {
    return request(url, method, null, new HashMap<String, String>(), classz);
  }

  private <T> T request(URL url, String method, byte[] body,
                            Map<String, String> headers, Type classz) throws IOException, RestClientException {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod(method);

      // connection.getResponseCode() implicitly calls getInputStream, so always set to true.
      // On the other hand, leaving this out breaks nothing.
      connection.setDoInput(true);

      // Include the headers in the request.
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        connection.setRequestProperty(entry.getKey(), entry.getValue());
      }

      // Set the connection settings.
      connection.setConnectTimeout(connectionTimeout);
      connection.setReadTimeout(readTimeout);
      connection.setUseCaches(false);
      connection.setRequestProperty("Accept", acceptEncoding);

      // set the body if available.
      if (body != null) {
        connection.setDoOutput(true);
        OutputStream os = null;
        try {
          os = connection.getOutputStream();
          os.write(body);
          os.flush();
        } catch (IOException e) {
          throw e;
        } finally {
          if (os != null) {
            os.close();
          }
        }
      }

      // Request, check status code and convert the response into object.
      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        InputStream is = connection.getInputStream();
        T result =  gson.fromJson(new InputStreamReader(is), classz);
        is.close();
        return result;
      } else if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
        return null;
      } else if (responseCode == HttpURLConnection.HTTP_INTERNAL_ERROR) {
        InputStream es = connection.getErrorStream();
        Response message;
        try {
          message = gson.fromJson(new InputStreamReader(es), Response.class);
        } catch (JsonSyntaxException | JsonIOException e) {
          throw new RestClientException(responseCode, e.getMessage());
        } finally {
          es.close();
        }
        throw new RestClientException(message.getStatus(), message.getMessage());
      } else {
        InputStream es = connection.getErrorStream();
        String response = IOUtils.toString(es, StandardCharsets.UTF_8);
        es.close();
        throw new RestClientException(responseCode, response);
      }
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }
}
