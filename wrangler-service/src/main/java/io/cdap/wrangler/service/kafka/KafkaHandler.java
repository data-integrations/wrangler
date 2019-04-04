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

package io.cdap.wrangler.service.kafka;

import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.RequestExtractor;
import io.cdap.wrangler.SamplingMethod;
import io.cdap.wrangler.ServiceUtils;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.dataset.connections.ConnectionStore;
import io.cdap.wrangler.dataset.workspace.DataType;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.dataset.workspace.WorkspaceMeta;
import io.cdap.wrangler.proto.ConnectionSample;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.PluginSpec;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.kafka.KafkaSpec;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.utils.ObjectSerDe;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

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

/**
 * Service for handling Kafka connections.
 */
public final class KafkaHandler extends AbstractWranglerHandler {

  @POST
  @Path("contexts/{context}/connections/kafka/test")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void test(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("context") String namespace) {
    respond(request, responder, () -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.KAFKA);

      KafkaConfiguration config = new KafkaConfiguration(connection);
      Properties props = config.get();

      // Checks connection by extracting topics.
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        consumer.listTopics();
      }
      return new ServiceResponse<Void>(String.format("Successfully connected to Kafka at %s", config.getConnection()));
    });
  }

  /**
   * List all kafka topics.
   *
   * @param request HTTP requests handler.
   * @param responder HTTP response handler.
   */
  @POST
  @Path("contexts/{context}/connections/kafka")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void list(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.KAFKA);

      KafkaConfiguration config = new KafkaConfiguration(connection);
      Properties props = config.get();

      // Extract topics from Kafka.
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        return new ServiceResponse<>(topics.keySet());
      }
    });
  }

  /**
   * Reads a kafka topic into workspace.
   *
   * @param request HTTP requests handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which the tables need to be listed from database.
   */
  @GET
  @Path("contexts/{context}/connections/{id}/kafka/{topic}/read")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void read(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                   @PathParam("id") String id, @PathParam("topic") String topic,
                   @QueryParam("lines") int lines,
                   @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      ConnectionStore store = ConnectionStore.get(context);
      WorkspaceDataset ws = WorkspaceDataset.get(context);
      Connection connection = getValidatedConnection(store, new NamespacedId(ns, id), ConnectionType.KAFKA);

      KafkaConfiguration config = new KafkaConfiguration(connection);
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config.get());
      consumer.subscribe(Lists.newArrayList(topic));
      String uuid = ServiceUtils.generateMD5(String.format("%s:%s.%s", scope, id, topic));
      NamespacedId namespacedId = new NamespacedId(ns, uuid);

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
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(namespacedId, topic)
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
        ws.updateWorkspaceData(namespacedId, DataType.RECORDS, data);

        ConnectionSample sample = new ConnectionSample(uuid, topic, ConnectionType.KAFKA.getType(),
                                                       SamplingMethod.FIRST.getMethod(), id);
        return new ServiceResponse<>(sample);
      } finally {
        consumer.close();
      }
    }));
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the connection.
   * @param topic for which the specification need to be generated.
   */
  @GET
  @Path("contexts/{context}/connections/{id}/kafka/{topic}/specification")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace,
                            @PathParam("id") String id, @PathParam("topic") String topic) {
    respond(request, responder, namespace, ns -> {
      Connection conn = getValidatedConnection(new NamespacedId(ns, id), ConnectionType.KAFKA);
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
      return new ServiceResponse<>(kafkaSpec);
    });
  }
}
