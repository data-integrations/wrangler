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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A step that invokes HTTP endpoint.
 */
@Plugin(type = "udd")
@Name("invoke-http")
@Usage("invoke-http <url> <column>[,<column>*] <header>[,<header>*]")
@Description("[EXPERIMENTAL] Invokes an HTTP endpoint, passing columns as a JSON map (potentially slow)")
public class InvokeHttp extends AbstractStep {
  private final String url;
  private final List<String> columns;
  private final Gson gson = new Gson();
  private final Map<String, String> headers = new HashMap<>();

  public InvokeHttp(int lineno, String detail, String url, List<String> columns, String hdrs)
    throws DirectiveParseException {
    super(lineno, detail);
    this.url = url;
    this.columns = columns;
    if (hdrs != null && !hdrs.isEmpty()) {
      String[] parsedHeaders = hdrs.split(",");
      for (String header : parsedHeaders) {
        String[] components = header.split("=");
        if (components.length != 2) {
          throw new DirectiveParseException (
            String.format("Incorrect header '%s' specified. " +
                            "Header should be specified as 'key=value' pairs separated by a comma (,).", header)
          );
        }
        String key = components[0].trim();
        String value = components[1].trim();
        if (key.isEmpty()) {
          throw new DirectiveParseException(
            String.format("Key specified for header '%s' cannot be empty.", header)
          );
        }
        if (value.isEmpty()) {
          throw new DirectiveParseException(
            String.format("Value specified for header '%s' cannot be empty.", header)
          );
        }
        headers.put(key, value);
      }
    }
  }

  /**
   * Character based cut operations
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Record} in which the 'col' value is lower cased.
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context)
    throws StepException, ErrorRecordException {
    for (Record record : records) {
      Map<String, Object> parameters = new HashMap<>();
      for (String column : columns) {
        int idx = record.find(column);
        if (idx != -1) {
          parameters.put(column, record.getValue(idx));
        }
      }
      try {
        Map<String, Object> result = InvokeHttp(url, parameters, headers);
        for(Map.Entry<String, Object> entry : result.entrySet()) {
          record.addOrSet(entry.getKey(), entry.getValue());
        }
      } catch (Exception e) {
        // If there are any issues, they will be pushed on the error port.
        throw new ErrorRecordException(e.getMessage(), 500);
      }
    }
    return records;
  }

  private class ServiceResponseHandler implements ResponseHandler<Map<String, Object>> {
    @Override
    public Map<String, Object> handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
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
      Map<String, Object> v = gson.fromJson(reader, new TypeToken<Map<String, Object>>(){}.getType());
      return v;
    }
  }

  private Map<String, Object> InvokeHttp(String url, Map<String, Object> parameters,
                                         Map<String, String> headers) throws IOException {
    CloseableHttpClient client = null;
    try {
      String body = gson.toJson(parameters);
      HttpPost post = new HttpPost(url);
      post.addHeader("Content-type", "application/json; charset=UTF-8");
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        post.addHeader(entry.getKey(), entry.getValue());
      }
      BasicHttpEntity entity = new BasicHttpEntity();
      InputStream stream = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
      entity.setContent(stream);
      post.setEntity(entity);
      client = HttpClients.createDefault();
      return client.execute(post, new ServiceResponseHandler());
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
