package co.cask.wrangler.service.bigquery;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.service.gcp.GCPServiceAccount;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import static co.cask.wrangler.ServiceUtils.error;


public class BigQueryService extends AbstractHttpServiceHandler {
  private static final String PROJECT_ID = "projectId";
  private static final String SERVICE_ACCOUNT_KEYFILE = "service-account-keyfile";
  private ConnectionStore store;

  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
  private Table connectionTable;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(connectionTable);
  }



  private BigQuery getBigQuery(Connection connection) throws Exception {
    Map<String, Object> properties = connection.getAllProps();
    if (properties.get(PROJECT_ID) == null) {
      throw new Exception("Configuration does not include project id.");
    }

    if (properties.get(SERVICE_ACCOUNT_KEYFILE) == null) {
      throw new Exception("Configuration does not include path to service account file.");
    }

    String path = (String) properties.get("service-account-keyfile");
    String projectId = (String) properties.get("projectId");
    ServiceAccountCredentials credentials = GCPServiceAccount.loadLocalFile(path);

    return BigQueryOptions.newBuilder()
      .setProjectId(projectId)
      .setCredentials(credentials)
      .build()
      .getService();
  }

  /**
   * List all Datasets.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("connections/{connection-id}/bigquery")
  public void listDatasets(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("connection-id") String connectionId) throws Exception {
    Connection connection = store.get(connectionId);

    if (!validateConnection(connectionId, connection, responder)) {
      return;
    }
    BigQuery bigQuery = getBigQuery(connection);
    Page<Dataset> datasets = bigQuery.listDatasets(BigQuery.DatasetListOption.all());
    List<JsonObject> allDatasets = new ArrayList<>();
    for (Dataset dataset : datasets.iterateAll()) {
      JsonObject object =  new JsonObject();
      object.addProperty("name", dataset.getDatasetId().getDataset());
      object.addProperty("created", dataset.getCreationTime());
      object.addProperty("description", dataset.getDescription());
      object.addProperty("last-modified", dataset.getLastModified());
      object.addProperty("location", dataset.getLocation());
//      StringBuilder builder = new StringBuilder();
//      for (Acl acl : dataset.getAcl()) {
//        String owner = "unknown";
//        Acl.Entity entity = acl.getEntity();
//        Acl.Entity.Type type = entity.getType();
//        if (type == Acl.Entity.Type.USER) {
//          owner = ((Acl.User) entity).getEmail();
//        } else if (type == Acl.Entity.Type.DOMAIN) {
//          owner = ((Acl.Domain) entity).getDomain();
//        } else if (type == Acl.Entity.Type.GROUP) {
//          owner = ((Acl.Group) entity).getIdentifier();
//        } else if (type == Acl.Entity.Type.VIEW) {
//          owner = ((Acl.View) entity).getId().getTable();
//        }
//        builder.append(owner).append(",");
//      }
//      builder.deleteCharAt(builder.length() - 1);
//      object.addProperty("owner", builder.toString());
      allDatasets.add(object);
    }
    responder.sendJson(allDatasets);
  }

  /**
   * List all Datasets.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("connections/{connection-id}/bigquery/{dataset-id}/tables")
  public void listDatasets(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("connection-id") String connectionId,
                           @PathParam("dataset-id") String datasetId) throws Exception {
    Connection connection = store.get(connectionId);

    if (!validateConnection(connectionId, connection, responder)) {
      return;
    }
    BigQuery bigQuery = getBigQuery(connection);
    Page<com.google.cloud.bigquery.Table> tablePage = bigQuery.listTables(datasetId);

    List<JsonObject> allTables = new ArrayList<>();

    for (com.google.cloud.bigquery.Table table : tablePage.iterateAll()) {
      JsonObject object = new JsonObject();

      object.addProperty("name", table.getFriendlyName());
      object.addProperty("table-id", table.getTableId().getTable());
      object.addProperty("created", table.getCreationTime());
      object.addProperty("description", table.getDescription());
      object.addProperty("last-modified", table.getLastModifiedTime());
      object.addProperty("expiration-time", table.getExpirationTime());
      object.addProperty("etag", table.getEtag());

      allTables.add(object);
    }

    responder.sendJson(allTables);
  }

  /**
   * List all tables in a dataset.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
//  @GET
//  @Path("connections/{connection-id}/bigquery/{dataset-id}/tables")
//  public void listTables(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("dataset-id") String datasetId) {
//    Dataset dataset = bigquery.getDataset(datasetId);
//
//
//  }

  private boolean validateConnection(String connectionId, Connection connection,
                                     HttpServiceResponder responder) {
    if (connection == null) {
      error(responder, "Unable to find connection in store for the connection id - " + connectionId);
      return false;
    }
    if (ConnectionType.BIGQUERY != connection.getType()) {
      error(responder, "Invalid connection type set, this endpoint only accepts bigquery connection type");
      return false;
    }
    return true;
  }
}
