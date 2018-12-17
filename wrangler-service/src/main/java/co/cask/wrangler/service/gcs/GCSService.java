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

import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.BytesDecoder;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.proto.PluginSpec;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.gcs.GCSBucketInfo;
import co.cask.wrangler.proto.gcs.GCSConnectionSample;
import co.cask.wrangler.proto.gcs.GCSObjectInfo;
import co.cask.wrangler.proto.gcs.GCSSpec;
import co.cask.wrangler.service.FileTypeDetector;
import co.cask.wrangler.service.common.AbstractWranglerService;
import co.cask.wrangler.service.common.Format;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.service.gcp.GCPUtils;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;

/**
 * Service to explore <code>GCS</code> filesystem
 */
public class GCSService extends AbstractWranglerService {
  private static final Logger LOG = LoggerFactory.getLogger(GCSService.class);
  static final int FILE_SIZE = 10 * 1024 * 1024;
  private FileTypeDetector detector;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    Security.addProvider(new BouncyCastleProvider());
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
  public void testGCSConnection(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent(Charsets.UTF_8.name(), Connection.class);
      if (connection == null) {
        responder.sendError(400, "Connection information is missing from the request body.");
        return;
      }
      ConnectionType connectionType = ConnectionType.fromString(connection.getType().getType());
      if (connectionType == ConnectionType.UNDEFINED || connectionType != ConnectionType.GCS) {
        error(responder,
              String.format("Invalid connection type %s set, expected 'GCS' connection type.",
                            connectionType.getType()));
        return;
      }

      GCPUtils.validateProjectCredentials(connection);

      Storage storage = GCPUtils.getStorageService(connection);
      storage.list(Storage.BucketListOption.pageSize(1));
      ServiceUtils.success(responder, "Success");
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
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
                         @QueryParam("path") String path,
                         @QueryParam("limit") @DefaultValue("1000") int objectLimit) {
    String bucketName = "";
    String prefix = null;

    try {
      final Connection[] connections = new Connection[1];
      getContext().execute(datasetContext -> connections[0] = store.get(connectionId));

      if (!validateConnection(connectionId, connections[0], responder)) {
        return;
      }

      Connection connection = connections[0];
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

      Storage storage = GCPUtils.getStorageService(connection);
      Set<String> bucketWhitelist = getBucketWhitelist(connection);

      if (bucketName.isEmpty() && prefix == null) {
        List<GCSBucketInfo> values = new ArrayList<>();
        // TODO: Remove objectLimit once CDAP-14446 is fixed.
        Buckets buckets = getBuckets(storage, bucketWhitelist, objectLimit);
        for (Bucket bucket : buckets.getBuckets()) {
          String name = bucket.getName();
          Acl.Entity entity = bucket.getOwner();
          Acl.Entity.Type type = entity == null ? null : entity.getType();
          String owner = "unknown";
          if (type == Acl.Entity.Type.USER) {
            owner = ((Acl.User) entity).getEmail();
          } else if (type == Acl.Entity.Type.DOMAIN) {
            owner = ((Acl.Domain) entity).getDomain();
          } else if (type == Acl.Entity.Type.PROJECT) {
            owner = ((Acl.Project) entity).getProjectId();
          }
          String fileType = detector.detectFileType(name.substring(name.lastIndexOf("/") + 1));
          boolean isWrangeable = detector.isWrangleable(fileType);
          // TODO: seems like file type and isWrangleable should not be done for buckets... is the UI ignoring these?
          GCSBucketInfo bucketInfo = new GCSBucketInfo(name, fileType, owner, bucket.getMetageneration(),
                                                       bucket.getGeneratedId(), bucket.getCreateTime() / 1000,
                                                       isWrangeable);
          values.add(bucketInfo);
        }
        ServiceResponse<GCSBucketInfo> response = new ServiceResponse<>(values, buckets.isLimitExceeded());
        responder.sendJson(response);
        return;
      }

      Page<Blob> list;
      if (prefix == null) {
        list = storage.list(bucketName, Storage.BlobListOption.currentDirectory());
      } else {
        list = storage.list(bucketName, Storage.BlobListOption.currentDirectory(),
                            Storage.BlobListOption.prefix(prefix));
      }

      Iterator<Blob> iterator = list.iterateAll().iterator();
      List<GCSObjectInfo> values = new ArrayList<>();
      boolean limitExceeded = false;
      while (iterator.hasNext()) {
        Blob blob = iterator.next();
        String p = String.format("/%s/%s", bucketName, blob.getName());
        if (p.equalsIgnoreCase(path)) {
          continue;
        }
        String name = new File(blob.getName()).getName();
        String bucket = blob.getBucket();
        String blobName = blob.getName();
        Long generation = blob.getGeneration();

        if (blob.isDirectory()) {
          values.add(new GCSObjectInfo(name, bucket, p, blobName, generation, true));
        } else {
          String fileType = detector.detectFileType(blob.getName());
          boolean isWrangeable = detector.isWrangleable(fileType);
          values.add(new GCSObjectInfo(name, bucket, p, blobName, generation, false, fileType,
                                       blob.getCreateTime() / 1000, blob.getUpdateTime() / 1000, blob.getSize(),
                                       isWrangeable));
        }
        if (values.size() >= objectLimit) {
          limitExceeded = true;
          break;
        }
      }
      ServiceResponse<GCSObjectInfo> response = new ServiceResponse<>(values, limitExceeded);
      responder.sendJson(response);
    } catch (Exception e) {
      LOG.warn(String.format("Listing failure for bucket '%s', prefix '%s'", bucketName, prefix), e);
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
  @GET
  @Path("/connections/{connection-id}/gcs/buckets/{bucket}/read")
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("connection-id") String connectionId,
                         @PathParam("bucket") String bucket,
                         @QueryParam("blob") final String blobPath,
                         @QueryParam("scope") String scope) {

    String contentType = request.getHeader(PropertyIds.CONTENT_TYPE);

    try {
      if (Strings.isNullOrEmpty(blobPath)) {
        responder.sendError(
          HttpURLConnection.HTTP_BAD_REQUEST,
          "Required query param 'path' is missing in the input"
        );
        return;
      }

      if (scope == null || scope.isEmpty()) {
        scope = WorkspaceDataset.DEFAULT_SCOPE;
      }

      Connection connection = store.get(connectionId);
      if (!validateConnection(connectionId, connection, responder)) {
        return;
      }

      Map<String, String> properties = new HashMap<>();
      Storage storage = GCPUtils.getStorageService(connection);
      Blob blob = storage.get(BlobId.of(bucket, blobPath));
      if (blob == null) {
        throw new Exception(String.format(
          "Bucket '%s', Path '%s' is not valid.", bucket, blobPath
        ));
      }

      String blobName = blob.getName();
      String id = ServiceUtils.generateMD5(String.format("%s:%s", scope, blobName));
      File file = new File(blobName);

      if (!blob.isDirectory()) {
        byte[] bytes = readGCSFile(blob, Math.min(blob.getSize().intValue(), GCSService.FILE_SIZE));
        ws.createWorkspaceMeta(id, scope, file.getName());

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
          ws.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.RECORDS, records);
          properties.put(PropertyIds.FORMAT, Format.TEXT.name());
        } else if (contentType.equalsIgnoreCase("application/json")) {
          ws.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.TEXT, bytes);
          properties.put(PropertyIds.FORMAT, Format.TEXT.name());
        } else if (contentType.equalsIgnoreCase("application/xml")) {
          ws.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.TEXT, bytes);
          properties.put(PropertyIds.FORMAT, Format.BLOB.name());
        } else {
          ws.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.BINARY, bytes);
          properties.put(PropertyIds.FORMAT, Format.BLOB.name());
        }

        // Set all properties and write to workspace.
        properties.put(PropertyIds.FILE_NAME, file.getCanonicalPath());
        properties.put(PropertyIds.URI, String.format("gs://%s/%s", bucket, blobPath));
        properties.put(PropertyIds.FILE_PATH, blobPath);
        properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.GCS.getType());
        properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
        properties.put(PropertyIds.CONNECTION_ID, connectionId);
        properties.put("bucket", bucket);
        ws.writeProperties(id, properties);

        // Preparing return response to include mandatory fields : id and name.
        GCSConnectionSample connectionSample =
          new GCSConnectionSample(id, file.getName(), ConnectionType.GCS.getType(), SamplingMethod.NONE.getMethod(),
                                  connectionId, String.format("gs://%s/%s", bucket, blobPath), blobPath, blobName,
                                  bucket);

        ServiceResponse<GCSConnectionSample> response = new ServiceResponse<>(connectionSample);
        responder.sendJson(response);
      } else {
        error(responder, HttpURLConnection.HTTP_BAD_REQUEST, "Path specified is not a file.");
      }
    } catch (Exception e) {
      LOG.warn(
        String.format("Read path '%s', bucket '%s' failed.", blobPath, bucket),
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

    if (workspaceId == null) {
      responder.sendError(400, "Workspace ID must be passed as query parameter 'wid'.");
      return;
    }
    try {

      Connection connection = store.get(connectionId);
      if (!validateConnection(connectionId, connection, responder)) {
        return;
      }

      Map<String, String> config = ws.getProperties(workspaceId);
      String formatStr = config.getOrDefault(PropertyIds.FORMAT, Format.TEXT.name());
      Format format = Format.valueOf(formatStr);
      String uri = config.get(PropertyIds.URI);
      String[] parts = uri.split("/");
      String filename = parts[parts.length - 1];
      String externalDatasetName = new StringJoiner(".").add(config.get("bucket")).add(filename).toString();

      Map<String, String> properties = new HashMap<>();
      properties.put("format", format.name().toLowerCase());
      properties.put("referenceName", externalDatasetName);
      properties.put("serviceFilePath", connection.getProp(GCPUtils.SERVICE_ACCOUNT_KEYFILE));
      properties.put("project", GCPUtils.getProjectId(connection));
      properties.put("path", uri);
      properties.put("recursive", "false");
      properties.put("filenameOnly", "false");
      properties.put("copyHeader", String.valueOf(shouldCopyHeader(workspaceId)));
      properties.put("schema", format.getSchema().toString());
      PluginSpec pluginSpec = new PluginSpec("GCSFile", "source", properties);
      GCSSpec spec = new GCSSpec(pluginSpec);

      ServiceResponse<GCSSpec> response = new ServiceResponse<>(spec);
      responder.sendJson(response);
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  private Set<String> getBucketWhitelist(Connection connection) {
    String whitelistStr = connection.getAllProps().get("bucketWhitelist");
    Set<String> whitelist = new LinkedHashSet<>();
    if (whitelistStr == null) {
      return whitelist;
    }
    for (String bucket : whitelistStr.split(",")) {
      whitelist.add(bucket.trim());
    }
    return whitelist;
  }

  private Buckets getBuckets(Storage storage, Set<String> whitelist, int objectLimit) {
    // this will include buckets that can be listed by the service account, but may not include all buckets
    // in the whitelist, if the whitelist contains publicly accessible buckets from other projects.
    // do some post-processing to filter out anything not in the whitelist and also try and lookup buckets
    // that are in the whitelist but not in the returned list
    Page<Bucket> list = storage.list();
    List<Bucket> output = new ArrayList<>();
    boolean limitExceeded = false;
    Set<String> missingBuckets = new HashSet<>(whitelist);
    // use iterateAll to make sure we get all results
    for (Bucket bucket : list.iterateAll()) {
      String bucketName = bucket.getName();
      missingBuckets.remove(bucketName);
      if (whitelist.isEmpty() || whitelist.contains(bucketName)) {
        if (output.size() >= objectLimit) {
          limitExceeded = true;
          break;
        }
        output.add(bucket);
      }
    }
    // this only contains buckets that are in the whitelist but were not returned by the list call
    for (String whitelistBucket : missingBuckets) {
      try {
        Bucket bucket = storage.get(whitelistBucket);
        if (bucket != null) {
          if (output.size() >= objectLimit) {
            limitExceeded = true;
            break;
          }
          output.add(bucket);
        }
      } catch (StorageException e) {
        // ignore and move on
        LOG.debug("Exception getting bucket {} from the whitelist.", whitelistBucket, e);
      }
    }

    return new Buckets(output, limitExceeded);
  }
}
