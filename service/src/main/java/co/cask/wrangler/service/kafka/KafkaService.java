package co.cask.wrangler.service.kafka;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;
import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

/**
 * Class description here.
 */
public final class KafkaService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);
  private static final Gson gson =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset ws;

  // Data Prep store which stores all the information associated with dataprep.
  @UseDataSet(DataPrep.DATAPREP_DATASET)
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

  @PUT
  @Path("connections/kafka/{id}/test")
  public void test(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("id") String id) {
    try {
      // Retrieve connection information for Kafka.
      Connection connection = store.get(id);
      KafkaConfiguration config = new KafkaConfiguration(connection);
      Properties props = config.get();

      // Checks connection by extracting topics.
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
      try {
        consumer.listTopics();
      } finally {
        consumer.close();
      }
      ServiceUtils.success(responder, "Success");
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  @GET
  @Path("connections/kafka/{id}/list")
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    try {
      // Retrieve connection information for Kafka.
      Connection connection = store.get(id);
      KafkaConfiguration config = new KafkaConfiguration(connection);
      Properties props = config.get();

      // Extract topics from Kafka.
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
      try {
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();

        // Prepare response.
        JsonArray values = new JsonArray();
        for(String topic : topics.keySet()) {
          values.add(new JsonPrimitive(topic));
        }

        JsonObject response = new JsonObject();
        response.addProperty("status", HttpURLConnection.HTTP_OK);
        response.addProperty("message", "Success");
        response.addProperty("count", values.size());
        response.add("values", values);
        ServiceUtils.sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
      } finally {
        consumer.close();
      }
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  @GET
  @Path("connections/kafka/{id}/read")
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("id") String id, @QueryParam("lines") int lines) {
    try {
      Connection connection = store.get(id);
      KafkaConfiguration config = new KafkaConfiguration(connection);
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config.get());
      consumer.subscribe(Lists.newArrayList(config.getTopic()));
      String uuid = ServiceUtils.generateMD5(id);
      ws.createWorkspaceMeta(uuid, id);
      try {
        boolean running = true;
        List<Record> recs = new ArrayList<>();
        int count = lines;
        while(running) {
          ConsumerRecords<String, String> records = consumer.poll(10000);
          for(ConsumerRecord<String, String> record : records) {
            Record rec = new Record();
            rec.add("timestamp", record.timestamp());
            rec.add("key", record.key());
            rec.add("offset", record.offset());
            rec.add("partition", record.partition());
            rec.add("value", record.value());
            recs.add(rec);
            if (count < 0) {
              break;
            }
            count--;
          }
          running = false;
        }
        // Set all properties and write to workspace.
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyIds.TOPIC, config.getTopic());
        properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
        ws.writeProperties(id, properties);

        String data = gson.toJson(recs);
        ws.writeToWorkspace(uuid, WorkspaceDataset.DATA_COL, DataType.RECORDS, Bytes.toBytes(data));

        JsonObject response = new JsonObject();
        JsonArray values = new JsonArray();
        JsonObject object = new JsonObject();
        object.addProperty(PropertyIds.ID, uuid);
        object.addProperty(PropertyIds.NAME, id);
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
}
