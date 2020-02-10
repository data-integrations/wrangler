/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.directives.parser;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Charsets;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.clients.RestClientException;
import io.cdap.wrangler.clients.SchemaRegistryClient;
import io.cdap.wrangler.codec.BinaryAvroDecoder;
import io.cdap.wrangler.codec.Decoder;
import io.cdap.wrangler.codec.DecoderException;
import io.cdap.wrangler.codec.JsonAvroDecoder;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A step to parse AVRO json or binary format.
 */
@Plugin(type = Directive.TYPE)
@Name("parse-as-avro")
@Categories(categories = { "parser", "avro"})
@Description("Parses column as AVRO generic record.")
public class ParseAvro implements Directive, Lineage {
  public static final String NAME = "parse-as-avro";
  private String column;
  private String schemaId;
  private String type;
  private long version;
  private Decoder<Row> decoder;
  private boolean decoderInitialized = false;
  private SchemaRegistryClient client;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("schema-id", TokenType.IDENTIFIER);
    builder.define("encode-type", TokenType.IDENTIFIER);
    builder.define("version", TokenType.NUMERIC, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.schemaId = ((Identifier) args.value("schema-id")).value();
    this.type = ((Identifier) args.value("encode-type")).value();
    if (!"json".equalsIgnoreCase(type) && !"binary".equalsIgnoreCase(type)) {
      throw new DirectiveParseException(
        NAME, String.format("Invalid encoding type '%s'. The type must be either 'json' or 'binary'.", type));
    }
    if (args.contains("version")) {
      this.version = ((Numeric) args.value("version")).value().intValue();
    } else {
      this.version = -1;
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    List<Row> results = new ArrayList<>();

    if (!decoderInitialized) {
      // Retryer callable, that allows this step attempt to connect to schema registry service
      // before giving up.
      Callable<Decoder<Row>> decoderCallable = () -> {
        client = SchemaRegistryClient.getInstance(context);
        byte[] bytes;
        if (version != -1) {
          bytes = client.getSchema(context.getNamespace(), schemaId, version);
        } else {
          bytes = client.getSchema(context.getNamespace(), schemaId);
        }
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(Bytes.toString(bytes));
        if ("json".equalsIgnoreCase(type)) {
          return new JsonAvroDecoder(schema);
        } else if ("binary".equalsIgnoreCase(type)) {
          return new BinaryAvroDecoder(schema);
        }
        return null;
      };

      // Retryer that retries when there is connection issue or any request / response
      // issue. It would exponentially back-off till wait time of 10 seconds is reached
      // for 5 attempts.
      Retryer<Decoder<Row>> retryer = RetryerBuilder.<Decoder<Row>>newBuilder()
        .retryIfExceptionOfType(IOException.class)
        .retryIfExceptionOfType(RestClientException.class)
        .withWaitStrategy(WaitStrategies.exponentialWait(10, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(5))
        .build();

      try {
        decoder = retryer.call(decoderCallable);
        if (decoder == null) {
          throw new DirectiveExecutionException(NAME, "Avro parsing is supported for 'json' and 'binary' types only.");
        }

        decoderInitialized = true;
      } catch (ExecutionException | RetryException e) {
        throw new DirectiveExecutionException(
          NAME, String.format("Unable to retrieve schema from schema registry. %s", e.getMessage()), e);
      }
    }

    try {
      for (Row row : rows) {
        int idx = row.find(column);
        if (idx != -1) {
          Object object = row.getValue(idx);
          if (object instanceof byte[]) {
            byte[] bytes = (byte[]) object;
            results.addAll(decoder.decode(bytes));
          } else if (object instanceof String) {
            String body = (String) object;
            byte[] bytes = body.getBytes(Charsets.UTF_8);
            results.addAll(decoder.decode(bytes));
          } else {
            throw new ErrorRowException(
              NAME, "Column " + column + " should be of type 'String' or 'byte array'.", 1);
          }
        }
      }
    } catch (DecoderException e) {
      throw new ErrorRowException(NAME, "Issue decoding Avro record. Check schema version '"
        + (version == -1 ? "latest" : version) + "'. " + e.getMessage(), 2);
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Parsed column '%s' as a Avro record", column)
      .all(Many.columns(column))
      .build();
  }
}
