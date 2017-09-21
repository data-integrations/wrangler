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
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.connections.ConnectionType;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;
import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

/**
 * Service to explore S3 filesystem
 */
public class S3Service extends AbstractHttpServiceHandler {
  private static final Gson gson =
    new GsonBuilder().
      setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DASHES).
      registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private static final int FILE_SIZE = 10 * 1024 * 1024;
  // Abstraction over the table defined above for managing connections.
  private ConnectionStore store;

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset table;

  @UseDataSet(DataPrep.DATAPREP_DATASET)
  private Table connectionTable;

  /**
   * Stores the context so that it can be used later.
   *
   * @param context the HTTP service runtime context
   */
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(connectionTable);
  }

  /**
   * Tests S3 Connection.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("/connections/s3/test")
  public void testS3Connection(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent(Charsets.UTF_8.name(), Connection.class);
      ConnectionType connectionType = ConnectionType.fromString(connection.getType().getType());
      if (connectionType == ConnectionType.UNDEFINED || connectionType != ConnectionType.S3) {
        error(responder,
              String.format("Invalid connection type %s set, expected 'S3' connection type.",
                            connectionType.getType()));
        return;
      }
      intializeAndGetS3Client(connection);
      ServiceUtils.success(responder, "Success");
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  private boolean validateConnection(String connectionId, Connection connection, HttpServiceResponder responder) {
    if (connection == null) {
      error(responder, "Unable to find connection in store for the connection id - " + connectionId);
      return false;
    }
    if (ConnectionType.S3 != connection.getType()) {
      error(responder, "Invalid connection type set, this endpoint only accepts S3 connection type");
      return false;
    }
    return true;
  }

  // creates s3 client and sets region and returns the initialized client
  private AmazonS3 intializeAndGetS3Client(Connection connection) {
    S3Configuration s3Configuration = new S3Configuration(connection);
    AmazonS3 s3 = new AmazonS3Client(s3Configuration);
    Region region = Region.getRegion(Regions.fromName(s3Configuration.getRegion()));
    s3.setRegion(region);
    return s3;
  }

  /**
   * Lists S3 buckets owned by the user identified by the credentials.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("/connections/{connection-id}/s3/buckets")
  public void listS3Buckets(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("connection-id") String connectionId) {
    try {
      Connection connection = store.get(connectionId);
      if (!validateConnection(connectionId, connection, responder)) {
        return;
      }
      AmazonS3 s3 = intializeAndGetS3Client(connection);
      responder.sendJson(200, s3.listBuckets(), new TypeToken<List<Bucket>>(){}.getType(), gson);
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * Lists S3 bucket's contents for the given prefix path.
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("/connections/{connection-id}/s3/buckets/{bucket-name}/explore")
  public void getS3BucketInfo(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("connection-id") String connectionId,
                              @PathParam("bucket-name") String bucketName,
                              @QueryParam("prefix") final String prefix) {
    try {
      Connection connection = store.get(connectionId);
      if (!validateConnection(connectionId, connection, responder)) {
        return;
      }
      AmazonS3 s3 = intializeAndGetS3Client(connection);
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
      listObjectsRequest.setBucketName(bucketName);
      listObjectsRequest.setPrefix(prefix);
      listObjectsRequest.setDelimiter("/");
      ObjectListing result;
      DirectoryListing listing = new DirectoryListing();
      do {
        result = s3.listObjects(listObjectsRequest);
        listing.addDirectories(result.getCommonPrefixes());
        listing.addObjects(result.getObjectSummaries());
        listObjectsRequest.setMarker(result.getMarker());
      } while(result.isTruncated() == true );

      responder.sendJson(200, listing, DirectoryListing.class, gson);
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * Reads s3 object into workspace
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("/connections/{connection-id}/s3/buckets/{bucket-name}/read")
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("connection-id") String connectionId,
                         @PathParam("bucket-name") String bucketName,
                         @QueryParam("key") final String key) {
    try {
      if (Strings.isNullOrEmpty(key)) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Required query param 'key' is missing in the input");
        return;
      }
      Connection connection = store.get(connectionId);
      if (!validateConnection(connectionId, connection, responder)) {
        return;
      }
      AmazonS3 s3 = intializeAndGetS3Client(connection);
      S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
      if (object != null) {
        try (InputStream inputStream = object.getObjectContent()) {
          loadFile(responder, inputStream, object);
        }
      } else {
        ServiceUtils.error(responder,
                           String.format("S3 Object with key %s and bucket-name %s is not found", key, bucketName));
        return;
      }
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param bucketName S3 object's bucket name
   * @param key S3 object's key
   */
  @Path("/connections/{connection-id}/s3/buckets/{bucket-name}/specification")
  @GET
  public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                            @PathParam("connection-id") String connectionId,
                            @PathParam("bucket-name") String bucketName, @QueryParam("key") final String key) {
    JsonObject response = new JsonObject();
    try {
      Connection conn = store.get(connectionId);
      S3Configuration s3Configuration = new S3Configuration(conn);
      JsonObject value = new JsonObject();
      JsonObject s3 = new JsonObject();

      Map<String, String> properties = new HashMap<>();
      properties.put("accessID", s3Configuration.getAWSAccessKeyId());
      properties.put("accessKey", s3Configuration.getAWSSecretKey());
      properties.put("path", String.format("s3://%s/%s", bucketName, key));

      s3.add("properties", gson.toJsonTree(properties));
      s3.addProperty("name", "S3");
      s3.addProperty("type", "source");
      value.add("S3", s3);

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

  private void loadFile(HttpServiceResponder responder, InputStream inputStream, S3Object s3Object) {
    JsonObject response = new JsonObject();
    BufferedInputStream stream = null;
    try {

      if (s3Object.getObjectMetadata().getContentLength() > FILE_SIZE) {
        error(responder, "Files greater than 10MB are not supported.");
        return;
      }

      // Creates workspace.
      String name = s3Object.getKey();

      String id = String.format("%s:%s", s3Object.getBucketName(), s3Object.getKey());
      id = ServiceUtils.generateMD5(id);
      table.createWorkspaceMeta(id, name);

      stream = new BufferedInputStream(inputStream);
      byte[] bytes = new byte[(int)s3Object.getObjectMetadata().getContentLength() + 1];
      stream.read(bytes);

      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put("bucket-name", s3Object.getBucketName());
      properties.put("key", s3Object.getKey());
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, id);
      table.writeProperties(id, properties);

      DataType dataType = DataType.fromString(s3Object.getObjectMetadata().getContentType());
      dataType = dataType == null ? DataType.BINARY : dataType;
      table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, dataType, bytes);

      // Preparing return response to include mandatory fields : id and name.
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty("bucket-name", s3Object.getBucketName());
      object.addProperty("key", s3Object.getKey());
      object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      values.add(object);

      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("id", id);
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e){
      error(responder, e.getMessage());
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          // Nothing much we can do here.
        }
      }
    }
  }

  /**
   * Constructed and returned while exploring the S3 fs
   */
  private class DirectoryListing {
    List<S3ObjectSummary> objectSummaries;
    List<String> directories;
    private DirectoryListing() {
      this.objectSummaries = new ArrayList<>();
      this.directories = new ArrayList<>();
    }

    private void addObjects(List<S3ObjectSummary> objectSummaries) {
      this.objectSummaries.addAll(objectSummaries);
    }
    private void addDirectories(List<String> dirs) {
      this.directories.addAll(dirs);
    }
  }
}
