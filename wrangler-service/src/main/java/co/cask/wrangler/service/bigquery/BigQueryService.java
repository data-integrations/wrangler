package co.cask.wrangler.service.bigquery;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


public class BigQueryService extends AbstractHttpServiceHandler {
  private ConnectionStore store;
  private BigQuery bigquery;

  @UseDataSet(DataPrep.DATAPREP_DATASET)
  private Table connectionTable;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(connectionTable);
    bigquery = BigQueryOptions.getDefaultInstance().getService();
  }

  /**
   * List all Datasets.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("connections/{connection-id}/bigquery")
  public void listDatasets(HttpServiceRequest request, HttpServiceResponder responder) {
    Page<Dataset> datasets = bigquery.listDatasets(BigQuery.DatasetListOption.all());
    for (Dataset dataset : datasets.iterateAll()) {
      dataset.list()
    }
  }

  /**
   * List all tables in a dataset.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("connections/{connection-id}/bigquery/{dataset-id}/tables")
  public void listDatasets(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("dataset-id") String datasetId) {
    Dataset dataset = bigquery.getDataset(datasetId);


  }
}
