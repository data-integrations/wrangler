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

package co.cask.wrangler.service.kafka;

import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.connections.ConnectionNotFoundException;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceMeta;
import co.cask.wrangler.proto.ConnectionSample;
import co.cask.wrangler.proto.PluginSpec;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionMeta;
import co.cask.wrangler.proto.connection.ConnectionType;
import co.cask.wrangler.proto.kafka.KafkaSpec;
import co.cask.wrangler.service.common.AbstractWranglerHandler;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;

/**
 * Service for handling Kafka connections.
 */
public final class KafkaHandler extends AbstractWranglerHandler {

  /**
   * Tests the connection to kafka..
   *
   * Following is the response when the connection is successful.
   *
   * {
   *   "status" : 200,
   *   "message" : "Successfully connected to kafka."
   * }
   *
   * @param request  HTTP request handler.
   * @param responder HTTP response handler.
   */
  @POST
  @Path("connections/kafka/test")
  public void test(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.KAFKA);

      KafkaConfiguration config = new KafkaConfiguration(connection);
      Properties props = config.get();

      // Checks connection by extracting topics.
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        consumer.listTopics();
      }
      ServiceUtils.success(responder, String.format("Successfully connected to Kafka at %s", config.getConnection()));
    } catch (IllegalArgumentException e) {
      ServiceUtils.error(responder, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * List all kafka topics.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
  @POST
  @Path("connections/kafka")
  public void list(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.KAFKA);

      KafkaConfiguration config = new KafkaConfiguration(connection);
      Properties props = config.get();

      // Extract topics from Kafka.
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        ServiceResponse<String> response = new ServiceResponse<>(topics.keySet());
        responder.sendJson(response);
      }
    } catch (IllegalArgumentException e) {
      ServiceUtils.error(responder, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * Reads a kafka topic into workspace.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which the tables need to be listed from database.
   */
  @GET
  @Path("connections/{id}/kafka/{topic}/read")
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("id") String id, @PathParam("topic") String topic,
                   @QueryParam("lines") int lines,
                   @QueryParam("scope") @DefaultValue("default") String scope) {
    try {
      Connection connection = store.get(id);

      KafkaConfiguration config = new KafkaConfiguration(connection);
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config.get());
      consumer.subscribe(Lists.newArrayList(topic));
      String uuid = ServiceUtils.generateMD5(String.format("%s:%s.%s", scope, id, topic));

      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.ID, uuid);
      properties.put(PropertyIds.NAME, topic);
      properties.put(PropertyIds.CONNECTION_ID, id);
      properties.put(PropertyIds.TOPIC, topic);
      properties.put(PropertyIds.BROKER, config.getConnection());
      properties.put(PropertyIds.CONNECTION_TYPE, connection.getType().getType());
      properties.put(PropertyIds.KEY_DESERIALIZER, config.getKeyDeserializer());
      properties.put(PropertyIds.VALUE_DESERIALIZER, config.getValueDeserializer());
      properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.FIRST.getMethod());
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(uuid, topic)
        .setScope(scope)
        .setProperties(properties)
        .build();
      ws.writeWorkspaceMeta(workspaceMeta);

      try {
        boolean running = true;
        List<Row> recs = new ArrayList<>();
        int count = lines;
        while (running) {
          ConsumerRecords<String, String> records = consumer.poll(10000);
          for (ConsumerRecord<String, String> record : records) {
            Row rec = new Row();
            rec.add("body", record.value());
            recs.add(rec);
            if (count < 0) {
              break;
            }
            count--;
          }
          running = false;
        }

        ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
        byte[] data = serDe.toByteArray(recs);
        ws.updateWorkspaceData(uuid, DataType.RECORDS, data);

        ConnectionSample sample = new ConnectionSample(uuid, topic, ConnectionType.KAFKA.getType(),
                                                       SamplingMethod.FIRST.getMethod(), id);
        ServiceResponse<ConnectionSample> response = new ServiceResponse<>(sample);
        responder.sendJson(response);
      } finally {
        consumer.close();
      }
    } catch (ConnectionNotFoundException e) {
      ServiceUtils.notFound(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the connection.
   * @param topic for which the specification need to be generated.
   */
  @Path("connections/{id}/kafka/{topic}/specification")
  @GET
  public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                            @PathParam("id") String id, @PathParam("topic") final String topic) {
    try {
      Connection conn = store.get(id);
      Map<String, String> connProperties = conn.getProperties();

      Map<String, String> properties = new HashMap<>();
      properties.put("topic", topic);
      properties.put("referenceName", topic);
      properties.put("brokers", connProperties.get(PropertyIds.BROKER));
      properties.put("kafkaBrokers", connProperties.get(PropertyIds.BROKER));
      properties.put("keyField", connProperties.get(PropertyIds.KEY_DESERIALIZER));
      properties.put("format", "text");

      PluginSpec pluginSpec = new PluginSpec("Kafka", "source", properties);
      KafkaSpec kafkaSpec = new KafkaSpec(pluginSpec);
      ServiceResponse<KafkaSpec> response = new ServiceResponse<>(kafkaSpec);
      responder.sendJson(response);
    } catch (ConnectionNotFoundException e) {
      ServiceUtils.notFound(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }
}
