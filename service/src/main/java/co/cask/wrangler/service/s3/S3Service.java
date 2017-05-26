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

package co.cask.wrangler.service.s3;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.service.kafka.KafkaService;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

/**
 * Manages connections with S3.
 */
public class S3Service extends AbstractHttpServiceHandler {
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

  /**
   * Tests S3 Connection.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("connections/s3/test")
  public void testS3Connection(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent("utf-8", Connection.class);

      if (ConnectionType.fromString(connection.getType().getType()) == ConnectionType.UNDEFINED) {
        error(responder, "Invalid connection type set.");
        return;
      }

      AWSCredentials credentials = new WebCredentialProvider(connection).getCredentials();
      AmazonS3 s3 = new AmazonS3Client(credentials);
      Region region = Region.getRegion(Regions.fromName((String)connection.getProp("region")));
      s3.setRegion(region);
      ServiceUtils.success(responder, "Success");
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * Lists the buckets with the S3 connection.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("connections/s3")
  public void listBuckets(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent("utf-8", Connection.class);

      if (ConnectionType.fromString(connection.getType().getType()) == ConnectionType.UNDEFINED) {
        error(responder, "Invalid connection type set.");
        return;
      }

      AWSCredentials credentials = new WebCredentialProvider(connection).getCredentials();
      AmazonS3 s3 = new AmazonS3Client(credentials);
      Region region = Region.getRegion(Regions.fromName((String)connection.getProp("region")));
      s3.setRegion(region);

      JsonObject response = new JsonObject();
      JsonArray values = new JsonArray();
      for(Bucket bucket :s3.listBuckets()) {
        JsonObject object = new JsonObject();
        object.addProperty("name", bucket.getName());
        object.addProperty("owner", bucket.getOwner().getDisplayName());
        object.addProperty("creation", bucket.getCreationDate().toString());
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
}
