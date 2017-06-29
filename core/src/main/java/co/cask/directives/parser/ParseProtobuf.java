/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.parser;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Identifier;
import co.cask.wrangler.api.parser.Numeric;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.clients.RestClientException;
import co.cask.wrangler.clients.SchemaRegistryClient;
import co.cask.wrangler.codec.Decoder;
import co.cask.wrangler.codec.DecoderException;
import co.cask.wrangler.codec.ProtobufDecoderUsingDescriptor;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A step to parse Protobuf encoded memory representations.
 */
@Plugin(type = Directive.Type)
@Name("parse-as-protobuf")
@Usage("parse-as-protobuf <column> <schema-id> <record-name> [version]")
@Description("Parses column as protobuf encoded memory representations.")
public class ParseProtobuf implements Directive {
  public static final String NAME = "parse-as-protobuf";
  private static final Logger LOG = LoggerFactory.getLogger(ParseProtobuf.class);
  private String column;
  private String schemaId;
  private String recordName;
  private long version;
  private Decoder<Row> decoder;
  private boolean decoderInitialized = false;
  private SchemaRegistryClient client;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("schema-id", TokenType.IDENTIFIER);
    builder.define("record-name", TokenType.TEXT);
    builder.define("version", TokenType.NUMERIC, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.schemaId = ((Identifier) args.value("schema-id")).value();
    this.recordName = ((Text) args.value("record-name")).value();
    if(args.contains("version")) {
      this.version = ((Numeric) args.value("version")).value().intValue();
    } else {
      this.version = -1;
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, final RecipeContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();

    if (!decoderInitialized) {
      // Retryer callable, that allows this step attempt to connect to schema registry service
      // before giving up.
      Callable<Decoder<Row>> decoderCallable = new Callable<Decoder<Row>>() {
        @Override
        public Decoder<Row> call() throws Exception {
          client = SchemaRegistryClient.getInstance(context);
          byte[] bytes;
          if (version != -1) {
            bytes = client.getSchema(schemaId, version);
          } else {
            bytes = client.getSchema(schemaId);
          }

          return new ProtobufDecoderUsingDescriptor(bytes, recordName);
        }
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
        if (decoder != null) {
          decoderInitialized = true;
        } else {
          throw new DirectiveExecutionException("Unsupported protobuf decoder type");
        }
      } catch (ExecutionException e) {
        throw new DirectiveExecutionException(
          String.format("Unable to retrieve protobuf descriptor from schema registry. %s", e.getCause())
        );
      } catch (RetryException e) {
        throw new DirectiveExecutionException(
          String.format("Issue in retrieving protobuf descriptor from schema registry. %s", e.getCause())
        );
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
          } else {
            throw new DirectiveExecutionException(toString() + " : column " + column + " should be of type byte array");
          }
        }
      }
    } catch (DecoderException e) {
      throw new DirectiveExecutionException(toString() + " Issue decoding Protobuf record. Check schema version '" +
                                (version == -1 ? "latest" : version) + "'. " + e.getMessage());
    }
    return results;
  }
}
