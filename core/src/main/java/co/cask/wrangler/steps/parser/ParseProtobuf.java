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
import co.cask.wrangler.clients.RestClientException;
import co.cask.wrangler.clients.SchemaRegistryClient;
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
@Usage(
  directive = "parse-as-protobuf",
  usage = "parse-as-protobuf <column> <schema-id> <record-name> [version]",
  description = "Parses column as protobuf encoded memory representations."
)
public class ParseProtobuf extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(ParseProtobuf.class);
  private final String column;
  private final String schemaId;
  private final String recordName;
  private final long version;
  private Decoder<Record> decoder;
  private boolean decoderInitialized = false;
  private SchemaRegistryClient client;

  public ParseProtobuf(int lineno, String directive, String column, String schemaId, String recordName, long version) {
    super(lineno, directive);
    this.column = column;
    this.schemaId = schemaId;
    this.recordName = recordName;
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
  public List<Record> execute(List<Record> records, final PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();

    if (!decoderInitialized) {
      // Retryer callable, that allows this step attempt to connect to schema registry service
      // before giving up.
      Callable<Decoder<Record>> decoderCallable = new Callable<Decoder<Record>>() {
        @Override
        public Decoder<Record> call() throws Exception {
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
      Retryer<Decoder<Record>> retryer = RetryerBuilder.<Decoder<Record>>newBuilder()
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
          throw new StepException("Unsupported protobuf decoder type");
        }
      } catch (ExecutionException e) {
        throw new StepException(
          String.format("Unable to retrieve protobuf descriptor from schema registry. %s", e.getCause())
        );
      } catch (RetryException e) {
        throw new StepException(
          String.format("Issue in retrieving protobuf descriptor from schema registry. %s", e.getCause())
        );
      }
    }

    try {
      for (Record record : records) {
        int idx = record.find(column);
        if (idx != -1) {
          Object object = record.getValue(idx);
          if (object instanceof byte[]) {
            byte[] bytes = (byte[]) object;
            results.addAll(decoder.decode(bytes));
          } else {
            throw new StepException(toString() + " : column " + column + " should be of type byte array");
          }
        }
      }
    } catch (DecoderException e) {
      throw new StepException(toString() + " Issue decoding Protobuf record. Check schema version '" +
                                (version == -1 ? "latest" : version) + "'. " + e.getMessage());
    }
    return results;
  }
}
