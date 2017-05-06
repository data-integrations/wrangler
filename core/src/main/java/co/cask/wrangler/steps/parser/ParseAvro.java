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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.Decoder;
import co.cask.wrangler.api.DecoderException;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.codec.JsonAvroDecoder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * A Step to parse AVRO.
 */
@Usage(
  directive = "parse-as-avro",
  usage = "parse-as-avro <column> <json|binary> <schema-id> [version]",
  description = "Parses column as AVRO Generic Record."
)
public class ParseAvro extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(ParseAvro.class);
  private final String column;
  private final String schemaId;
  private final String type;
  private final long version;
  private Decoder<Record> decoder;
  private boolean decoderInitialized = false;
  private final Gson gson = new Gson();

  public ParseAvro(int lineno, String directive, String column, String schemaId, String type, long version) {
    super(lineno, directive);
    this.column = column;
    this.schemaId = schemaId;
    this.type = type;
    this.version = version;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records  Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();

    if (!decoderInitialized) {
      String path = "http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/schemas";
      //getSchemaRegistryService(context);
      decoder = initializeDecoder(schemaId, type, version, path);
      decoderInitialized = true;
    }

    try {
      for (Record record : records) {
        int idx = record.find(column);
        if (idx != -1) {
          Object object = record.getValue(idx);
          if (object instanceof byte[]) {
            byte[] bytes = (byte[]) object;
            results.addAll(decoder.decode(bytes, column));
          } else if (object instanceof String) {
            String body = (String) object;
            byte[] bytes = body.getBytes(Charsets.UTF_8);
            results.addAll(decoder.decode(bytes, column));
          } else {
            throw new StepException(toString() + " : column " + column + " should be of type string or byte array");
          }
        }
      }
    } catch (DecoderException e) {
      throw new StepException(toString() + " Issue decoding Avro record. Check schema version '" +
                                (version == -1 ? "latest" : version) + "'. " + e.getMessage());
    }
    return records;
  }

  private Decoder<Record> initializeDecoder(String schemaId, String type, long version, String url) {
    if (version != -1) {
      url = String.format("%s/%s/versions/%d", url, schemaId, version);
    } else {
      url = String.format("%s/%s", url, schemaId);
    }

    LOG.trace("Requesting schema registry with url '" + url + "'");
    HttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(url);
    try {
      HttpResponse response = client.execute(request);
      BufferedReader rd = new BufferedReader(
        new InputStreamReader(response.getEntity().getContent()));

      StringBuffer result = new StringBuffer();
      String line = "";
      while ((line = rd.readLine()) != null) {
        result.append(line);
      }

      JsonObject object = gson.fromJson(result.toString(), JsonObject.class);
      JsonArray array = (JsonArray) object.get("values");
      JsonObject o = (JsonObject) array.get(0);
      Schema.Parser parser = new Schema.Parser();
      Schema schema = parser.parse(o.get("specification").getAsString());
      return new JsonAvroDecoder(schema);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
