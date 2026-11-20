/*
 *  Copyright © 2020 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */
package io.cdap.wrangler.datamodel;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.wrangler.clients.RestClientException;
import io.cdap.wrangler.utils.AvroSchemaLoader;
import io.cdap.wrangler.utils.Manifest;
import io.cdap.wrangler.utils.OutboundUrlValidator;
import org.apache.avro.Schema;
import org.apache.commons.collections4.SetValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.function.Function;

/**
 * The {@link HTTPSchemaLoader} object loads an AVRO data model schema at a given url.
 */
public final class HTTPSchemaLoader implements AvroSchemaLoader {

  private static final Logger LOG = LoggerFactory.getLogger(HTTPSchemaLoader.class);
  private static final String FORMAT = "avsc";

  private final Gson gson;
  private final String baseUrl;
  private final String manifestName;

  // Connections settings.
  private int connectionTimeout;
  private int readTimeout;
  private String acceptEncoding;

  public HTTPSchemaLoader(String baseUrl, String manifestName) {
    URI validatedBaseUri = OutboundUrlValidator.validateOutboundUrl(baseUrl);
    OutboundUrlValidator.resolveSafeRelativePath(validatedBaseUri, manifestName);
    this.gson = new GsonBuilder().create();
    this.baseUrl = validatedBaseUri.toString();
    this.manifestName = manifestName.trim();
    this.connectionTimeout = 2000;
    this.readTimeout = 1000;
    this.acceptEncoding = "application/json";
  }

  /**
   * Loads the data models located at the url. All available data models should be
   * listed within a manifest.json file found at the root of the url. The
   * manifest.json should have the schema of {@link io.cdap.wrangler.utils.Manifest}.
   * If the loader is unable to download the manifest or referenced data models,
   * the load will throw an exception. Below is an example manifest file.
   *    {
   *      "standards": {
   *        "OMOP_6_0_0": {
   *          "format": "avsc"
   *        }
   *      }
   *    }
   * @return a map with keys representing the name of the schema. The value is a set of all of the revisions of the
   * schema.
   * @throws IOException when the manifest is missing or a parsing error.
   */
  @Override
  public SetValuedMap<String, Schema> load() throws IOException {
    Manifest manifest = downloadResource(baseUrl, manifestName,
                                         is -> gson.fromJson(new InputStreamReader(is), Manifest.class));
    if (manifest == null) {
      throw new IOException("unable to download the manifest resource.");
    }
    return downloadSchemas(manifest);
  }

  private SetValuedMap<String, Schema> downloadSchemas(Manifest manifest) throws IOException {
    SetValuedMap<String, Schema> avroSchemas = new HashSetValuedHashMap<>();
    if (manifest.getStandards() == null) {
      return avroSchemas;
    }

    // The AVRO parser treats meta data properties with a name that has an underscore prefix as invalid names while
    // the AVRO 1.9.1 spec states that underscore prefixes are valid. Consequently disabling validation.
    Schema.Parser parser = new Schema.Parser().setValidate(false);
    for (Map.Entry<String, Manifest.Standard> spec : manifest.getStandards().entrySet()) {
      if (spec.getValue().getFormat().equals(HTTPSchemaLoader.FORMAT)) {
        String resource = String.format("%s.%s", spec.getKey(), HTTPSchemaLoader.FORMAT);
        Schema schema = downloadResource(baseUrl, resource, is -> {
          try {
            return parser.parse(is);
          } catch (Exception e) {
            LOG.warn(String.format("Unable to parse resource %s due to error: %s", resource, e.getMessage()));
          }
          return null;
        });
        if (schema != null) {
          avroSchemas.put(schema.getFullName(), schema);
        }
      }
    }
    return avroSchemas;
  }

  private <R> R downloadResource(String baseUrl, String resourceName, Function<InputStream, R> callable) {
    R resource = null;
    try {
      URL resourceURL = concat(new URI(baseUrl), resourceName).toURL();
      resource = getRequest(resourceURL, callable);
    } catch (IOException e) {
      LOG.warn(String.format("Unable to download resource %s at %s", resourceName, baseUrl));
    } catch (RestClientException r) {
      LOG.warn(String.format("Failed to make request to due to error %s", r.getMessage()));
    } catch (Exception e) {
      LOG.warn(String.format("Unable to generate resource url for %s/%s", baseUrl, resourceName));
      return null;
    }
    return resource;
  }

  private <R> R getRequest(URL url, Function<InputStream, R> callable) throws IOException, RestClientException {
    URI requestUri = OutboundUrlValidator.validateSyntaxAndHost(url.toString());
    HttpGet httpGet = new HttpGet(requestUri);
    httpGet.addHeader("Accept", acceptEncoding);
    try (CloseableHttpClient client = OutboundUrlValidator.createSafeHttpClient(connectionTimeout, readTimeout);
         CloseableHttpResponse response = client.execute(httpGet)) {
      int responseCode = response.getStatusLine().getStatusCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
          return null;
        }
        try (InputStream is = entity.getContent()) {
          return callable.apply(is);
        }
      } else if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
        return null;
      } else {
        throw new RestClientException(
          responseCode, String.format("Request to %s failed with HTTP status %d.", url, responseCode));
      }
    }
  }

  private URI concat(URI uri, String extraPath) {
    return OutboundUrlValidator.resolveSafeRelativePath(uri, extraPath);
  }
}
