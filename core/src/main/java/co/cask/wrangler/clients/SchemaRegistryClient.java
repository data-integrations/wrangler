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

package co.cask.wrangler.clients;

import co.cask.wrangler.clients.entities.ErrorMessage;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Key;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class {@link SchemaRegistryClient} provides client API to interate with Schema Register Service.
 */
public final class SchemaRegistryClient {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryClient.class);
  private final String base;
  private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  static final JsonFactory JSON_FACTORY = new GsonFactory();
  private final HttpRequestFactory requestFactory;

  public static class DailyMotionUrl extends GenericUrl {
    public DailyMotionUrl(String encodedUrl) {
      super(encodedUrl);
    }

    @Key
    public String fields;
  }

  public SchemaRegistryClient(String base, final ExponentialBackOff backoff) {
    this.base = base;
    requestFactory =
      HTTP_TRANSPORT.createRequestFactory(new HttpRequestInitializer() {
        @Override
        public void initialize(HttpRequest request) {
          request.setParser(new JsonObjectParser(JSON_FACTORY));
          request.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backoff));
        }
      });
  }

  public SchemaRegistryClient(String base) {
    this(base, new ExponentialBackOff.Builder()
      .setInitialIntervalMillis(500)
      .setMaxElapsedTimeMillis(900000)
      .setMaxIntervalMillis(6000)
      .setMultiplier(1.5)
      .setRandomizationFactor(0.5)
      .build());
  }

  public List<Long> getSchemaVersions(String id) throws IOException, RestClientException {
    DailyMotionUrl url = new DailyMotionUrl("https://api.dailymotion.com/videos/");
    url.fields = "id,tags,title,url";
    HttpRequest request = requestFactory.buildGetRequest(url);
    String requestUrl = String.format("%s/schemas/%s/versions", base, id);
    JsonObject object = sendRequest(requestUrl, "GET", null, new HashMap<String, String>(), JsonObject.class);
    JsonArray array = (JsonArray) object.get("values");
    List<Long> result = new Gson().fromJson(array, List.class);
    return result;
  }

  private <T> T sendRequest(String requestUrl, String method, byte[] body,
                            Map<String, String> headers, Type classz) throws IOException, RestClientException {
    HttpURLConnection connection = null;
    try {
      URL url = new URL(requestUrl);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod(method);

      // connection.getResponseCode() implicitly calls getInputStream, so always set to true.
      // On the other hand, leaving this out breaks nothing.
      connection.setDoInput(true);

      for (Map.Entry<String, String> entry : headers.entrySet()) {
        connection.setRequestProperty(entry.getKey(), entry.getValue());
      }

      connection.setUseCaches(false);

      if (body != null) {
        connection.setDoOutput(true);
        OutputStream os = null;
        try {
          os = connection.getOutputStream();
          os.write(body);
          os.flush();
        } catch (IOException e) {
          LOG.error("Failed to send HTTP request to endpoint: " + url, e);
          throw e;
        } finally {
          if (os != null) {
            os.close();
          }
        }
      }

      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        InputStream is = connection.getInputStream();
        T result =  new Gson().fromJson(new InputStreamReader(is), classz);
        is.close();
        return result;
      } else if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
        return null;
      } else {
        InputStream es = connection.getErrorStream();
        ErrorMessage errorMessage;
        try {
          errorMessage = new Gson().fromJson(new InputStreamReader(es), ErrorMessage.class);
        } catch (JsonSyntaxException | JsonIOException e) {
          errorMessage = new ErrorMessage(8, e.getMessage());
        }
        es.close();
        throw new RestClientException(errorMessage.getMessage(), responseCode,
                                      errorMessage.getStatus());
      }

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }
}
