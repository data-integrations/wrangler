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

package co.cask.wrangler.service.gcs;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.BytesDecoder;
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
import co.cask.wrangler.service.FileTypeDetector;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.service.explorer.DatasetProvider;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
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
 * Service to explore <code>GCS</code> filesystem
 */
public class GCSService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(GCSService.class);
  private static final String PROJECT_ID = "projectId";
  private static final String SERVICE_ACCOUNT_KEYFILE = "service-account-keyfile";
  public static final int FILE_SIZE = 10 * 1024 * 1024;
  // Abstraction over the table defined above for managing connections.
  private ConnectionStore store;
  private DatasetProvider provider;
  private FileTypeDetector detector;

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset table;

  @UseDataSet(DataPrep.DATAPREP_DATASET)
  private Table connectionTable;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(connectionTable);
    final HttpServiceContext ctx = context;
    Security.addProvider(new BouncyCastleProvider());
    provider = new DatasetProvider() {
      @Override
      public Dataset acquire() {
        return ctx.getDataset("dataprepfs");
      }

      @Override
      public void release(Dataset dataset) {
        ctx.discardDataset(dataset);
      }
    };
    this.detector = new FileTypeDetector();
  }

  /**
   * Tests GCS Connection.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("/connections/gcs/test")
  public void testS3Connection(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent(Charsets.UTF_8.name(), Connection.class);
      ConnectionType connectionType = ConnectionType.fromString(connection.getType().getType());
      if (connectionType == ConnectionType.UNDEFINED || connectionType != ConnectionType.GCS) {
        error(responder,
              String.format("Invalid connection type %s set, expected 'GCS' connection type.",
                            connectionType.getType()));
        return;
      }

      Storage storage = getStorage(connection);
      storage.list(Storage.BucketListOption.pageSize(1));
      ServiceUtils.success(responder, "Success");
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  private Storage getStorage(Connection connection) throws Exception {
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

    Storage storage = StorageOptions.newBuilder()
      .setProjectId(projectId)
      .setCredentials(credentials)
      .build()
      .getService();

    return storage;
  }

  private boolean validateConnection(String connectionId, Connection connection,
                                     HttpServiceResponder responder) {
    if (connection == null) {
      error(responder, "Unable to find connection in store for the connection id - " + connectionId);
      return false;
    }
    if (ConnectionType.GCS != connection.getType()) {
      error(responder, "Invalid connection type set, this endpoint only accepts GCS connection type");
      return false;
    }
    return true;
  }

  /**
   * Lists GCS bucket's contents for the given prefix path.
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @ReadOnly
  @GET
  @Path("/connections/{connection-id}/gcs/explore")
  public void exploreGCS(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("connection-id") final String connectionId,
                              @QueryParam("path") String path) {
    String bucketName = "";
    String prefix = null;

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

      Storage storage = getStorage(connection[0]);
      if (bucketName.isEmpty() && prefix == null) {
        Page<Bucket> list = storage.list();
        Iterator<Bucket> iterator = list.getValues().iterator();
        JsonObject response = new JsonObject();
        response.addProperty("status", HttpURLConnection.HTTP_OK);
        response.addProperty("message", "OK");
        JsonArray values = new JsonArray();
        while(iterator.hasNext()) {
          com.google.cloud.storage.Bucket bucket = iterator.next();
          JsonObject object = new JsonObject();
          String name = bucket.getName();
          object.addProperty("name", name);
          object.addProperty("created", bucket.getCreateTime() / 1000);
          object.addProperty("generated-id", bucket.getGeneratedId());
          object.addProperty("meta-generation", bucket.getMetageneration());
          object.addProperty("type", "bucket");
          object.addProperty("directory", true);

          Acl.Entity entity = bucket.getOwner();
          Acl.Entity.Type type = entity.getType();
          if (type == Acl.Entity.Type.USER) {
            object.addProperty("owner", ((Acl.User) entity).getEmail());
          } else if (type == Acl.Entity.Type.DOMAIN) {
            object.addProperty("owner", ((Acl.Domain) entity).getDomain());
          } else if (type == Acl.Entity.Type.DOMAIN) {
            object.addProperty("owner", ((Acl.Project) entity).getProjectId());
          } else {
            object.addProperty("owner", "unknown");
          }

          boolean isWrangeable = false;
          try {
            String fileType = detector.detectFileType(name.substring(name.lastIndexOf("/") + 1));
            object.addProperty("type", fileType);
            isWrangeable = detector.isWrangleable(fileType);
          } catch (IOException e) {
            object.addProperty("type", FileTypeDetector.UNKNOWN);
            // We will not enable wrangling on unknown data.
          }
          object.addProperty("wrangle", isWrangeable);
          values.add(object);
        }
        response.addProperty("count", values.size());
        response.add("values", values);
        sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
        return;
      }

      Page<Blob> list = null;
      if (prefix == null) {
        list = storage.list(bucketName, Storage.BlobListOption.currentDirectory());
      } else {
        list = storage.list(bucketName, Storage.BlobListOption.currentDirectory(),
                            Storage.BlobListOption.prefix(prefix));
      }

      Iterator<Blob> iterator = list.iterateAll().iterator();
      JsonArray values = new JsonArray();
      while(iterator.hasNext()) {
        JsonObject object = new JsonObject();
        Blob blob = iterator.next();
        object.addProperty("bucket", blob.getBucket());
        object.addProperty("name", new File(blob.getName()).getName());
        object.addProperty("generation", blob.getGeneration());
        String p = String.format("/%s/%s", bucketName, blob.getName());
        if (p.equalsIgnoreCase(path)) {
          continue;
        }
        object.addProperty("path", p);
        if (blob.isDirectory()) {
          object.addProperty("directory", true);
        } else {
          object.addProperty("created", blob.getCreateTime() / 1000);
          object.addProperty("updated", blob.getUpdateTime() / 1000);
          object.addProperty("directory", false);
          object.addProperty("size", blob.getSize());
          boolean isWrangeable = false;
          try {
            String fileType = detector.detectFileType(blob.getName());
            object.addProperty("type", fileType);
            isWrangeable = detector.isWrangleable(fileType);
          } catch (IOException e) {
            object.addProperty("type", FileTypeDetector.UNKNOWN);
            // We will not enable wrangling on unknown data.
          }
          object.addProperty("wrangle", isWrangeable);
        }
        values.add(object);
      }
      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "OK");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      LOG.warn(
        String.format("Listing failure for bucket '%s', prefix '%s'", bucketName, prefix),
        e
      );
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  private byte[] readGCSFile(Blob blob, int len) throws IOException {
    try (ReadChannel reader = blob.reader()) {
      reader.setChunkSize(len);
      byte[] bytes = new byte[len];
      WritableByteChannel writable = Channels.newChannel(new ByteArrayOutputStream(len));
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      long total = len;
      while (reader.read(buf) != -1 && total > 0) {
        buf.flip();
        while (buf.hasRemaining()) {
          total -= writable.write(buf);
        }
        buf.clear();
      }
      return bytes;
    }
  }

  /**
   * Reads GCS object into workspace.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("/connections/{connection-id}/gcs/buckets/{bucket}/read")
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("connection-id") String connectionId,
                         @PathParam("bucket") String bucket,
                         @QueryParam("path") final String path) {

    RequestExtractor extractor = new RequestExtractor(request);
    String contentType = extractor.getHeader(RequestExtractor.CONTENT_TYPE_HEADER, null);

    try {
      if (Strings.isNullOrEmpty(path)) {
        responder.sendError(
          HttpURLConnection.HTTP_BAD_REQUEST,
          "Required query param 'path' is missing in the input"
        );
        return;
      }

      Connection connection = store.get(connectionId);
      if (!validateConnection(connectionId, connection, responder)) {
        return;
      }

      Map<String, String> properties = new HashMap<>();
      Storage storage = getStorage(connection);
      Blob blob = storage.get(BlobId.of(bucket, path));
      if (blob == null) {
        throw new Exception(String.format(
          "Bucket '%s', Path '%s' is not valid.", bucket, path
        ));
      }

      String blobName = blob.getName();
      String id = ServiceUtils.generateMD5(blobName);
      File file = new File(blobName);

      if (!blob.isDirectory()) {
        byte[] bytes = readGCSFile(blob, Math.min(blob.getSize().intValue(), GCSService.FILE_SIZE));
        table.createWorkspaceMeta(id, file.getName());

        String encoding = BytesDecoder.guessEncoding(bytes);
        if (contentType.equalsIgnoreCase("text/plain")
          && (encoding.equalsIgnoreCase("utf-8") || encoding.equalsIgnoreCase("ascii"))) {
          String data = new String(bytes, encoding);
          String[] lines = data.split("\r\n|\r|\n");
          if (blob.getSize() > GCSService.FILE_SIZE) {
            lines = Arrays.copyOf(lines, lines.length - 1);
            if (lines.length == 0) {
              throw new Exception("A single of text file is larger than " + FILE_SIZE + ", unable to process");
            }
          }

          List<Row> rows = new ArrayList<>();
          for (String line : lines) {
            rows.add(new Row("body", line));
          }

          ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
          byte[] records = serDe.toByteArray(rows);
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.RECORDS, records);
          properties.put(PropertyIds.PLUGIN_TYPE, "normal");
        } else if (contentType.equalsIgnoreCase("application/json")) {
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.JSON, bytes);
          properties.put(PropertyIds.PLUGIN_TYPE, "normal");
        } else if (contentType.equalsIgnoreCase("application/xml")) {
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.XML, bytes);
          properties.put(PropertyIds.PLUGIN_TYPE, "blob");
        } else {
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.BINARY, bytes);
          properties.put(PropertyIds.PLUGIN_TYPE, "blob");
        }

        // Set all properties and write to workspace.
        properties.put(PropertyIds.FILE_NAME, file.getCanonicalPath());
        properties.put(PropertyIds.URI, String.format("gs://%s/%s", bucket, path));
        properties.put(PropertyIds.FILE_PATH, path);
        properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.GCS.getType());
        properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
        properties.put(PropertyIds.CONNECTION_ID, connectionId);
        properties.put("bucket", bucket);
        table.writeProperties(id, properties);

        // Preparing return response to include mandatory fields : id and name.
        JsonArray values = new JsonArray();
        JsonObject object = new JsonObject();
        object.addProperty(PropertyIds.ID, id);
        object.addProperty(PropertyIds.NAME, file.getName());
        object.addProperty(PropertyIds.URI, String.format("gs://%s/%s", bucket, path));
        object.addProperty(PropertyIds.FILE_PATH, path);
        object.addProperty(PropertyIds.FILE_NAME, blobName);
        object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
        object.addProperty("bucket", bucket);
        values.add(object);

        JsonObject response = new JsonObject();
        response.addProperty("status", HttpURLConnection.HTTP_OK);
        response.addProperty("message", "Success");
        response.addProperty("count", values.size());
        response.add("values", values);
        sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
      } else {
        error(responder, HttpURLConnection.HTTP_BAD_REQUEST, "Path specified is not a file.");
      }
    } catch (Exception e) {
      LOG.warn(
        String.format("Read path '%s', bucket '%s' failed.", path, bucket),
        e
      );
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @Path("/connections/{connection-id}/gcs/specification")
  @GET
  public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                            @PathParam("connection-id") String connectionId,
                            @QueryParam("wid") String workspaceId) {

    JsonObject response = new JsonObject();
    try {

      Connection connection = store.get(connectionId);
      if (!validateConnection(connectionId, connection, responder)) {
        return;
      }

      Map<String, String> config = table.getProperties(workspaceId);
      String pluginType = config.get(PropertyIds.PLUGIN_TYPE);
      String bucket = config.get("bucket");
      String uri = config.get(PropertyIds.URI);
      String file = config.get(PropertyIds.FILE_NAME);

      Map<String, String> properties = new HashMap<>();
      JsonObject value = new JsonObject();

      if (pluginType.equalsIgnoreCase("normal")) {
        JsonObject gcs = new JsonObject();
        properties.put("referenceName", file);
        properties.put("serviceFilePath", (String) connection.getProp(SERVICE_ACCOUNT_KEYFILE));
        properties.put("project", (String) connection.getProp(PROJECT_ID));
        properties.put("bucket", bucket);
        properties.put("path", uri);
        properties.put("recursive", "false");
        properties.put("filenameOnly", "false");
        gcs.add("properties", new Gson().toJsonTree(properties));
        gcs.addProperty("name", "GCSFile");
        gcs.addProperty("type", "source");
        value.add("GCSFile", gcs);
      } else if (pluginType.equalsIgnoreCase("blob")) {
        JsonObject gcs = new JsonObject();
        properties.put("referenceName", file);
        properties.put("serviceFilePath", (String) connection.getProp(SERVICE_ACCOUNT_KEYFILE));
        properties.put("project", (String) connection.getProp(PROJECT_ID));
        properties.put("bucket", bucket);
        properties.put("path", uri);
        gcs.add("properties", new Gson().toJsonTree(properties));
        gcs.addProperty("name", "GCSFileBlob");
        gcs.addProperty("type", "source");
        value.add("GCSFileBlob", gcs);
      }
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
