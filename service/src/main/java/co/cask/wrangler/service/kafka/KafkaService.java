package co.cask.wrangler.service.kafka;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Class description here.
 */
public final class KafkaService extends AbstractHttpServiceHandler {
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

  }

  @GET
  @Path("connections/kafka/{id}/list")
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    try {
      Connection connection = store.get(id);
      KafkaProperties properties = new KafkaProperties(connection.getAllProps());
      Properties props = properties.get();
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
      Map<String, List<PartitionInfo>> topics = consumer.listTopics();
      JsonObject response = new JsonObject();
      JsonArray values = new JsonArray();
      for(Map.Entry<String, List<PartitionInfo>> entry : topics.entrySet()) {
        JsonObject object = new JsonObject();
        object.addProperty("name", entry.getKey());
        List<PartitionInfo> partitions = entry.getValue();
        JsonArray parts = new JsonArray();
        for (PartitionInfo info : partitions) {
          JsonObject part = new JsonObject();
          part.addProperty("partition", info.partition());
          part.addProperty("leader.host", info.leader().host());
          parts.add(part);
        }
        object.add("partitions", parts);
        values.add(object);
      }
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      ServiceUtils.sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  @GET
  @Path("connections/kafka/{id}/read")
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("id") String id) {
    try {
      Connection connection = store.get(id);
      KafkaProperties properties = new KafkaProperties(connection.getAllProps());


    } catch (Exception e) {

    }
  }

}
