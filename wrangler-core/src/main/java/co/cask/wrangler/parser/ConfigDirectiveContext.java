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

package co.cask.wrangler.parser;

import co.cask.wrangler.api.DirectiveConfig;
import co.cask.wrangler.api.DirectiveContext;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;

/**
 * This class {@link ConfigDirectiveContext} manages the context for directive
 * by either retrieving the configuration from a service or from a provided
 * instance of {@link DirectiveConfig}.
 */
public class ConfigDirectiveContext implements DirectiveContext {
  private DirectiveConfig config;

  public ConfigDirectiveContext(DirectiveConfig config) {
    this.config = config;
  }

  public ConfigDirectiveContext(URL url) throws IOException {
    CloseableHttpClient client = null;
    try {
      HttpGet get = new HttpGet(url.toString());
      get.addHeader("Content-type", "application/json; charset=UTF-8");
      client = HttpClients.createDefault();
      this.config = client.execute(get, new ServiceResponseHandler());
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  public ConfigDirectiveContext(String json) {
    Gson gson = new Gson();
    this.config = gson.fromJson(json, DirectiveConfig.class);
  }

  /**
   * Checks if the directive is aliased.
   *
   * @param directive to be checked for aliasing.
   * @return true if the directive has an alias, false otherwise.
   */
  @Override
  public boolean hasAlias(String directive) {
    return config.hasAlias(directive);
  }

  /**
   * Returns the root directive aliasee
   * @param directive
   * @return
   */
  @Override
  public String getAlias(String directive) {
    return config.getAliasName(directive);
  }

  /**
   * Checks if the directive is being excluded from being used.
   *
   * @param directive to be checked for exclusion.
   * @return true if excluded, false otherwise.
   */
  @Override
  public boolean isExcluded(String directive) {
    return config.isExcluded(directive);
  }

  private class ServiceResponseHandler implements ResponseHandler<DirectiveConfig> {
    @Override
    public DirectiveConfig handleResponse(HttpResponse response) throws ClientProtocolException,
      IOException {
      StatusLine statusLine = response.getStatusLine();
      HttpEntity entity = response.getEntity();
      if (statusLine.getStatusCode() >= 300) {
        throw new HttpResponseException(
          statusLine.getStatusCode(),
          statusLine.getReasonPhrase());
      }
      if (entity == null) {
        throw new ClientProtocolException("Response contains no content");
      }
      Gson gson = new GsonBuilder().create();
      Reader reader = new InputStreamReader(entity.getContent(), Charset.forName("UTF-8"));
      DirectiveConfig config = gson.fromJson(reader, DirectiveConfig.class);
      return config;
    }
  }
}
