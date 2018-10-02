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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.ReadWrite;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.DatasetContext;
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
import co.cask.wrangler.sampling.Bernoulli;
import co.cask.wrangler.sampling.Poisson;
import co.cask.wrangler.sampling.Reservoir;
import co.cask.wrangler.service.FileTypeDetector;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.service.explorer.BoundedLineInputStream;
import co.cask.wrangler.utils.ObjectSerDe;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
  private static final Logger LOG = LoggerFactory.getLogger(S3Service.class);
  private static final Gson gson =
    new GsonBuilder().
      setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_DASHES).
      registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private static final String COLUMN_NAME = "body";
  private static final int FILE_SIZE = 10 * 1024 * 1024;
  private static final int BUCKET_LIMIT = 10000;
  // Abstraction over the table defined above for managing connections.
  private ConnectionStore store;

  private FileTypeDetector detector;

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset table;

  @UseDataSet(DataPrep.CONNECTIONS_DATASET)
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
    this.detector = new FileTypeDetector();
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
      // creating a client doesn't test the connection, we will do list buckets so the connection is tested.
      intializeAndGetS3Client(connection).listBuckets();
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
   * Lists S3 bucket's contents for the given prefix path.
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @ReadOnly
  @GET
  @Path("/connections/{connection-id}/s3/explore")
  public void getS3BucketInfo(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("connection-id") final String connectionId,
                              @QueryParam("path") String path) {
    try {
      final Connection[] connection = new Connection[1];
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          connection[0] = store.get(connectionId);
        }
      });
      if (!validateConnection(connectionId, connection[0], responder)) {
        return;
      }

      String bucketName = "";
      String prefix = null;
      int bucketStart = path.indexOf("/");
      if (bucketStart != -1) {
        int bucketEnd = path.indexOf("/", bucketStart + 1);
        if (bucketEnd != -1) {
          bucketName = path.substring(bucketStart + 1, bucketEnd);
          if ((bucketEnd + 1) != path.length()) {
            prefix = path.substring(bucketEnd + 1);
          }
        } else {
          bucketName = path.substring(bucketStart + 1);
        }
      }

      AmazonS3 s3 = intializeAndGetS3Client(connection[0]);
      if (bucketName.isEmpty() && prefix == null) {
        List<Bucket> buckets = s3.listBuckets();
        JsonObject response = new JsonObject();
        response.addProperty("status", HttpURLConnection.HTTP_OK);
        response.addProperty("message", "OK");
        response.addProperty("count", buckets.size());
        JsonArray values = new JsonArray();
        for (Bucket bucket : buckets) {
          JsonObject object = new JsonObject();
          object.addProperty("name", bucket.getName());
          object.addProperty("created", bucket.getCreationDate().getTime());
          object.addProperty("owner", bucket.getOwner().getDisplayName());
          object.addProperty("type", "bucket");
          object.addProperty("directory", true);
          values.add(object);
        }
        response.add("values", values);
        sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
        return;
      }

      ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
      listObjectsRequest.setBucketName(bucketName);
      if (prefix != null) {
        listObjectsRequest.setPrefix(prefix);
      }
      listObjectsRequest.setDelimiter("/");
      ObjectListing result;
      DirectoryListing listing = new DirectoryListing();
      boolean limitExceeded = false;
      do {
        if (listing.size() == BUCKET_LIMIT) {
          limitExceeded = true;
          break;
        }
        result = s3.listObjects(listObjectsRequest);
        listing.addDirectory(result.getCommonPrefixes());
        listing.addObject(result.getObjectSummaries());
        listObjectsRequest.setMarker(result.getMarker());
      } while (result.isTruncated());

      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "OK");
      response.addProperty("count", listing.size());
      response.add("values", listing.get());
      if (limitExceeded) {
        response.addProperty("truncated", "true");
      }
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (AmazonS3Exception e) {
      ServiceUtils.error(responder, e.getStatusCode(), e.getMessage());
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
  @ReadWrite
  @Path("/connections/{connection-id}/s3/buckets/{bucket-name}/read")
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("connection-id") String connectionId,
                         @PathParam("bucket-name") String bucketName,
                         @QueryParam("key") final String key, @QueryParam("lines") int lines,
                         @QueryParam("sampler") String sampler, @QueryParam("fraction") double fraction,
                         @QueryParam("scope") String scope) {
    try {
      if (Strings.isNullOrEmpty(key)) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Required query param 'key' is missing in the input");
        return;
      }

      if (Strings.isNullOrEmpty(scope)) {
        scope = WorkspaceDataset.DEFAULT_SCOPE;
      }

      RequestExtractor extractor = new RequestExtractor(request);
      String header = extractor.getHeader(RequestExtractor.CONTENT_TYPE_HEADER, null);
      Connection connection = store.get(connectionId);
      if (!validateConnection(connectionId, connection, responder)) {
        return;
      }
      AmazonS3 s3 = intializeAndGetS3Client(connection);
      S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
      if (object != null) {
        try (InputStream inputStream = object.getObjectContent()) {
          if (header != null && header.equalsIgnoreCase("text/plain")) {
            loadSamplableFile(connection.getId(), responder, scope, inputStream, object, lines, fraction, sampler);
            return;
          }
          loadFile(connection.getId(), responder, inputStream, object);
        }
      } else {
        ServiceUtils.error(responder,
                           String.format("S3 Object with key %s and bucket-name %s is not found", key, bucketName));
        return;
      }
    } catch (AmazonS3Exception e) {
      ServiceUtils.error(responder, e.getStatusCode(), e.getMessage());
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
      properties.put("path", String.format("s3a://%s/%s", bucketName, key));

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

  private void loadSamplableFile(String connectionId, HttpServiceResponder responder,
                                 String scope, InputStream inputStream, S3Object s3Object,
                                 int lines, double fraction, String sampler) {
    JsonObject response = new JsonObject();
    SamplingMethod samplingMethod = SamplingMethod.fromString(sampler);
    if (sampler == null || sampler.isEmpty() || SamplingMethod.fromString(sampler) == null) {
      samplingMethod = SamplingMethod.FIRST;
    }

    try(BoundedLineInputStream blis = BoundedLineInputStream.iterator(inputStream, Charsets.UTF_8, lines)) {
      String name = s3Object.getKey();

      String file = String.format("%s:%s:%s", scope, s3Object.getBucketName(), s3Object.getKey());
      String identifier = ServiceUtils.generateMD5(file);
      String fileName = name.substring(name.lastIndexOf("/") + 1);
      table.createWorkspaceMeta(identifier, scope, fileName);

      // Iterate through lines to extract only 'limit' random lines.
      // Depending on the type, the sampling of the input is performed.
      List<Row> rows = new ArrayList<>();
      Iterator<String> it = blis;
      if (samplingMethod == SamplingMethod.POISSON) {
        it = new Poisson<String>(fraction).sample(blis);
      } else if (samplingMethod == SamplingMethod.BERNOULLI) {
        it = new Bernoulli<String>(fraction).sample(blis);
      } else if (samplingMethod == SamplingMethod.RESERVOIR) {
        it = new Reservoir<String>(lines).sample(blis);
      }
      while(it.hasNext()) {
        rows.add(new Row(COLUMN_NAME, it.next()));
      }

      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.ID, identifier);
      properties.put(PropertyIds.NAME, fileName);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, connectionId);
      // S3 specific properties.
      properties.put("bucket-name", s3Object.getBucketName());
      properties.put("key", s3Object.getKey());
      table.writeProperties(identifier, properties);

      // Write rows to workspace.
      ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
      byte[] data = serDe.toByteArray(rows);
      table.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL, DataType.RECORDS, data);

      // Preparing return response to include mandatory fields : id and name.
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty(PropertyIds.ID, identifier);
      object.addProperty(PropertyIds.NAME, name);
      object.addProperty(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
      object.addProperty(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
      object.addProperty(PropertyIds.CONNECTION_ID, connectionId);
      object.addProperty("bucket-name", s3Object.getBucketName());
      object.addProperty("key", s3Object.getKey());
      values.add(object);

      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (IOException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  private void loadFile(String connectionId, HttpServiceResponder responder, InputStream inputStream, S3Object s3Object) {
    JsonObject response = new JsonObject();
    BufferedInputStream stream = null;
    try {

      if (s3Object.getObjectMetadata().getContentLength() > FILE_SIZE) {
        error(responder, "Files greater than 10MB are not supported.");
        return;
      }

      // Creates workspace.
      String name = s3Object.getKey();

      String file = String.format("%s:%s", s3Object.getBucketName(), s3Object.getKey());
      String identifier = ServiceUtils.generateMD5(file);
      String fileName = name.substring(name.lastIndexOf("/") + 1);
      table.createWorkspaceMeta(identifier, fileName);

      stream = new BufferedInputStream(inputStream);
      byte[] bytes = new byte[(int)s3Object.getObjectMetadata().getContentLength() + 1];
      stream.read(bytes);

      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.ID, identifier);
      properties.put(PropertyIds.NAME, fileName);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, connectionId);

      // S3 specific properties.
      properties.put("bucket-name", s3Object.getBucketName());
      properties.put("key", s3Object.getKey());
      table.writeProperties(identifier, properties);
      table.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL, getDataType(name), bytes);

      // Preparing return response to include mandatory fields : id and name.
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty(PropertyIds.ID, identifier);
      object.addProperty(PropertyIds.NAME, name);
      object.addProperty(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
      object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      object.addProperty(PropertyIds.CONNECTION_ID, connectionId);
      object.addProperty("bucket-name", s3Object.getBucketName());
      object.addProperty("key", s3Object.getKey());
      values.add(object);

      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
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
    JsonArray objects;

    private DirectoryListing() {
      this.objects = new JsonArray();
    }

    private void addObject(List<S3ObjectSummary> summaries) {
      for (S3ObjectSummary summary : summaries) {
        JsonObject object = new JsonObject();
        int idx = summary.getKey().lastIndexOf("/");
        String name = summary.getKey();
        if (idx != -1) {
          name = name.substring(idx + 1);
        }
        object.addProperty("name", name);
        object.addProperty("path", summary.getKey());
        object.addProperty("directory", false);
        object.addProperty("last-modified", summary.getLastModified().getTime());
        object.addProperty("owner", summary.getOwner().getDisplayName());
        object.addProperty("size", summary.getSize());
        object.addProperty("class", summary.getStorageClass());
        boolean isWrangeable = false;
        try {
          String type = detector.detectFileType(name);
          object.addProperty("type", type);
          isWrangeable = detector.isWrangleable(type);
        } catch (IOException e) {
          object.addProperty("type", FileTypeDetector.UNKNOWN);
          // We will not enable wrangling on unknown data.
        }
        object.addProperty("wrangle", isWrangeable);
        objects.add(object);
      }
    }

    private void addDirectory(List<String> dirs) {
      for (String dir : dirs) {
        JsonObject object = new JsonObject();
        if (dir.equalsIgnoreCase("/")) {
          continue;
        }
        String[] parts = dir.split("/");
        String name = dir;
        if (parts.length > 1) {
          name = parts[parts.length - 1];
        }
        object.addProperty("name", name);
        object.addProperty("type", "directory");
        object.addProperty("directory", true);
        object.addProperty("path", dir);
        objects.add(object);
      }
    }

    private int size() {
      return objects.size();
    }

    private JsonArray get() {
      return objects;
    }
  }

  /**
   * get data type from the file type.
   * @param fileName
   * @return DataType
   * @throws IOException
   */
  private DataType getDataType(String fileName) throws IOException {
    // detect fileType from fileName
    String fileType = detector.detectFileType(fileName);
    DataType dataType = DataType.fromString(fileType);
    return dataType == null ? DataType.BINARY : dataType;
  }
}
