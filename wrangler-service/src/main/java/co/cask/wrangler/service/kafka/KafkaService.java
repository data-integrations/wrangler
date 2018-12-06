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

package co.cask.wrangler.service.kafka;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;
import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

/**
 * Service for handling Kafka connections.
 */
public final class KafkaService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);
  private static final Gson gson =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset ws;

  // Data Prep store which stores all the information associated with dataprep.
  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
  private Table table;

  // Abstraction over the table defined above for managing connections.
  private ConnectionStore store;

  /**
   * Stores the context so that it can be used later.
   *
   * @param context the HTTP service runtime context
   */
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(table);
  }

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
      Connection connection = extractor.getContent("utf-8", Connection.class);

      if (ConnectionType.fromString(connection.getType().getType()) == ConnectionType.UNDEFINED) {
        error(responder, "Invalid connection type set.");
        return;
      }

      KafkaConfiguration config = new KafkaConfiguration(connection);
      Properties props = config.get();

      // Checks connection by extracting topics.
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        consumer.listTopics();
      }
      ServiceUtils.success(responder, String.format("Successfully connected to Kafka at %s", config.getConnection()));
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
      Connection connection = extractor.getContent("utf-8", Connection.class);

      if (ConnectionType.fromString(connection.getType().getType()) == ConnectionType.UNDEFINED) {
        error(responder, "Invalid connection type set.");
        return;
      }

      KafkaConfiguration config = new KafkaConfiguration(connection);
      Properties props = config.get();

      // Extract topics from Kafka.
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();

        // Prepare response.
        JsonArray values = new JsonArray();
        for (String topic : topics.keySet()) {
          values.add(new JsonPrimitive(topic));
        }

        JsonObject response = new JsonObject();
        response.addProperty("status", HttpURLConnection.HTTP_OK);
        response.addProperty("message", "Success");
        response.addProperty("count", values.size());
        response.add("values", values);
        ServiceUtils.sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
      }
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
                   @QueryParam("scope") String scope) {
    try {
      Connection connection = store.get(id);
      if (connection == null) {
        error(responder, String.format("Invalid connection id '%s' specified or connection does not exist.", id));
        return;
      }

      if (scope == null || scope.isEmpty()) {
        scope = WorkspaceDataset.DEFAULT_SCOPE;
      }

      KafkaConfiguration config = new KafkaConfiguration(connection);
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config.get());
      consumer.subscribe(Lists.newArrayList(topic));
      String uuid = ServiceUtils.generateMD5(String.format("%s:%s.%s", scope, id, topic));
      ws.createWorkspaceMeta(uuid, scope, topic);

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
        ws.writeToWorkspace(uuid, WorkspaceDataset.DATA_COL, DataType.RECORDS, data);

        // Set all properties and write to workspace.
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
        ws.writeProperties(uuid, properties);

        JsonObject response = new JsonObject();
        JsonArray values = new JsonArray();
        JsonObject object = new JsonObject();
        object.addProperty(PropertyIds.ID, uuid);
        object.addProperty(PropertyIds.NAME, topic);
        values.add(object);
        response.addProperty("status", HttpURLConnection.HTTP_OK);
        response.addProperty("message", "Success");
        response.addProperty("count", values.size());
        response.add("values", values);
        sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
      } finally {
        consumer.close();
      }
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
    JsonObject response = new JsonObject();
    try {
      Connection conn = store.get(id);
      JsonObject value = new JsonObject();
      JsonObject kafka = new JsonObject();

      Map<String, String> properties = new HashMap<>();
      properties.put("topic", topic);
      properties.put("referenceName", topic);
      properties.put("brokers", conn.getProp(PropertyIds.BROKER));
      properties.put("kafkaBrokers", conn.getProp(PropertyIds.BROKER));
      properties.put("keyField", conn.getProp(PropertyIds.KEY_DESERIALIZER));
      properties.put("format", "text");

      kafka.add("properties", gson.toJsonTree(properties));
      kafka.addProperty("name", "Kafka");
      kafka.addProperty("type", "source");
      value.add("Kafka", kafka);

      JsonArray values = new JsonArray();
      values.add(value);
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }
}
