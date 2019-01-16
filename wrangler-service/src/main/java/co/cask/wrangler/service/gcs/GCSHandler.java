/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceMeta;
import co.cask.wrangler.proto.BadRequestException;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.PluginSpec;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionMeta;
import co.cask.wrangler.proto.connection.ConnectionType;
import co.cask.wrangler.proto.gcs.GCSBucketInfo;
import co.cask.wrangler.proto.gcs.GCSConnectionSample;
import co.cask.wrangler.proto.gcs.GCSObjectInfo;
import co.cask.wrangler.proto.gcs.GCSSpec;
import co.cask.wrangler.service.FileTypeDetector;
import co.cask.wrangler.service.common.AbstractWranglerHandler;
import co.cask.wrangler.service.common.Format;
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
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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

/**
 * Service to explore <code>GCS</code> filesystem
 */
public class GCSHandler extends AbstractWranglerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(GCSHandler.class);
  static final long FILE_SIZE = 10 * 1024 * 1024;
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
    respond(request, responder, () -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.GCS);
      GCPUtils.validateProjectCredentials(connection);

      Storage storage = GCPUtils.getStorageService(connection);
      storage.list(Storage.BucketListOption.pageSize(1));
      return new ServiceResponse<Void>("Success");
    });
  }

  private void validateConnection(String connectionId, Connection connection) {
    if (connection == null) {
      throw new BadRequestException("Unable to find connection in store for the connection id - " + connectionId);
    }
    if (ConnectionType.GCS != connection.getType()) {
      throw new BadRequestException("Invalid connection type set, this endpoint only accepts GCS connection type");
    }
  }

  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @ReadOnly
  @GET
  @Path("/connections/{connection-id}/gcs/explore")
  public void exploreGCS(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("connection-id") String connectionId,
                         @QueryParam("path") String path,
                         @QueryParam("limit") @DefaultValue("1000") int objectLimit) {
    exploreGCS(request, responder, getContext().getNamespace(), connectionId, path, objectLimit);
  }

  /**
   * Lists GCS bucket's contents for the given prefix path.
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @ReadOnly
  @GET
  @Path("contexts/{context}/connections/{connection-id}/gcs/explore")
  public void exploreGCS(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace,
                         @PathParam("connection-id") String connectionId,
                         @QueryParam("path") String path,
                         @QueryParam("limit") @DefaultValue("1000") int objectLimit) {
    respond(request, responder, namespace, () -> {
      String bucketName = "";
      String prefix = null;

      Connection connection = getValidatedConnection(NamespacedId.of(namespace, connectionId), ConnectionType.GCS);

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
        return new ServiceResponse<>(values, buckets.isLimitExceeded());
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
      return new ServiceResponse<>(values, limitExceeded);
    });
  }

  private byte[] readGCSFile(Blob blob, int len) throws IOException {
    try (ReadChannel reader = blob.reader();
      ByteArrayOutputStream os = new ByteArrayOutputStream(len)) {
      reader.setChunkSize(len);
      WritableByteChannel writable = Channels.newChannel(os);
      ByteBuffer buf = ByteBuffer.wrap(new byte[8192]);
      long total = len;
      while (reader.read(buf) != -1 && total > 0) {
        buf.flip();
        while (buf.hasRemaining()) {
          total -= writable.write(buf);
        }
        buf.clear();
      }
      return os.toByteArray();
    }
  }

  @GET
  @Path("/connections/{connection-id}/gcs/buckets/{bucket}/read")
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("connection-id") String connectionId,
                         @PathParam("bucket") String bucket,
                         @QueryParam("blob") String blobPath,
                         @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    loadObject(request, responder, getContext().getNamespace(), connectionId, bucket, blobPath, scope);
  }

  /**
   * Reads GCS object into workspace.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/gcs/buckets/{bucket}/read")
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace,
                         @PathParam("connection-id") String connectionId,
                         @PathParam("bucket") String bucket,
                         @QueryParam("blob") String blobPath,
                         @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, () -> {
      String contentType = request.getHeader(PropertyIds.CONTENT_TYPE);

      if (blobPath == null || blobPath.isEmpty()) {
        throw new BadRequestException("Required query param 'path' is missing in the input");
      }

      Connection connection = store.get(NamespacedId.of(namespace, connectionId));
      validateConnection(connectionId, connection);

      Map<String, String> properties = new HashMap<>();
      Storage storage = GCPUtils.getStorageService(connection);
      Blob blob = storage.get(BlobId.of(bucket, blobPath));
      if (blob == null) {
        throw new BadRequestException(String.format("Bucket '%s', Path '%s' is not valid.", bucket, blobPath));
      }

      String blobName = blob.getName();
      String id = ServiceUtils.generateMD5(String.format("%s:%s", scope, blobName));
      File file = new File(blobName);

      // Set all properties and write to workspace.
      properties.put(PropertyIds.FILE_NAME, file.getCanonicalPath());
      properties.put(PropertyIds.URI, String.format("gs://%s/%s", bucket, blobPath));
      properties.put(PropertyIds.FILE_PATH, blobPath);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.GCS.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, connectionId);
      properties.put("bucket", bucket);
      NamespacedId namespacedId = NamespacedId.of(namespace, id);
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(namespacedId, file.getName())
        .setScope(scope)
        .setProperties(properties)
        .build();
      ws.writeWorkspaceMeta(workspaceMeta);

      if (!blob.isDirectory()) {
        boolean shouldTruncate = blob.getSize() > FILE_SIZE;
        byte[] bytes = readGCSFile(blob, (int) (shouldTruncate ? FILE_SIZE : blob.getSize()));

        String encoding = BytesDecoder.guessEncoding(bytes);
        if (contentType.equalsIgnoreCase("text/plain")
          && (encoding.equalsIgnoreCase("utf-8") || encoding.equalsIgnoreCase("ascii"))) {
          String data = new String(bytes, encoding);
          String[] lines = data.split("\r\n|\r|\n");
          if (blob.getSize() > GCSHandler.FILE_SIZE) {
            lines = Arrays.copyOf(lines, lines.length - 1);
            if (lines.length == 0) {
              throw new BadRequestException("A single of text file is larger than "
                                              + FILE_SIZE + ", unable to process");
            }
          }

          List<Row> rows = new ArrayList<>();
          // if the content was truncated, ignore the last line because it's probably not complete.
          int numLines = shouldTruncate && lines.length > 1 ? lines.length - 1 : lines.length;
          for (int i = 0; i < numLines; i++) {
            rows.add(new Row("body", lines[i]));
          }

          ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
          byte[] records = serDe.toByteArray(rows);
          ws.updateWorkspaceData(namespacedId, DataType.RECORDS, records);
          properties.put(PropertyIds.FORMAT, Format.TEXT.name());
        } else if (contentType.equalsIgnoreCase("application/json")) {
          ws.updateWorkspaceData(namespacedId, DataType.TEXT, bytes);
          properties.put(PropertyIds.FORMAT, Format.TEXT.name());
        } else if (contentType.equalsIgnoreCase("application/xml")) {
          ws.updateWorkspaceData(namespacedId, DataType.TEXT, bytes);
          properties.put(PropertyIds.FORMAT, Format.BLOB.name());
        } else {
          ws.updateWorkspaceData(namespacedId, DataType.BINARY, bytes);
          properties.put(PropertyIds.FORMAT, Format.BLOB.name());
        }

        // Preparing return response to include mandatory fields : id and name.
        GCSConnectionSample connectionSample =
          new GCSConnectionSample(id, file.getName(), ConnectionType.GCS.getType(), SamplingMethod.NONE.getMethod(),
                                  connectionId, String.format("gs://%s/%s", bucket, blobPath), blobPath, blobName,
                                  bucket);

        return new ServiceResponse<>(connectionSample);
      } else {
        throw new BadRequestException("Path specified is not a file.");
      }
    });
  }

  @Path("/connections/{connection-id}/gcs/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("connection-id") String connectionId,
                            @QueryParam("wid") String workspaceId) {
    specification(request, responder, getContext().getNamespace(), connectionId, workspaceId);
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @Path("contexts/{context}/connections/{connection-id}/gcs/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace, @PathParam("connection-id") String connectionId,
                            @QueryParam("wid") String workspaceId) {
    respond(request, responder, namespace, () -> {
      if (workspaceId == null) {
        throw new BadRequestException("Workspace ID must be passed as query parameter 'wid'.");
      }

      Connection connection = store.get(NamespacedId.of(namespace, connectionId));
      validateConnection(connectionId, connection);

      NamespacedId namespacedIdWorkspaceId = NamespacedId.of(namespace, workspaceId);
      Map<String, String> config = ws.getWorkspace(namespacedIdWorkspaceId).getProperties();
      String formatStr = config.getOrDefault(PropertyIds.FORMAT, Format.TEXT.name());
      Format format = Format.valueOf(formatStr);
      String uri = config.get(PropertyIds.URI);
      String[] parts = uri.split("/");
      String filename = parts[parts.length - 1];
      String externalDatasetName = new StringJoiner(".").add(config.get("bucket")).add(filename).toString();

      Map<String, String> properties = new HashMap<>();
      properties.put("format", format.name().toLowerCase());
      properties.put("referenceName", externalDatasetName);
      properties.put("serviceFilePath", connection.getProperties().get(GCPUtils.SERVICE_ACCOUNT_KEYFILE));
      properties.put("project", GCPUtils.getProjectId(connection));
      properties.put("path", uri);
      properties.put("recursive", "false");
      properties.put("filenameOnly", "false");
      properties.put("copyHeader", String.valueOf(shouldCopyHeader(namespacedIdWorkspaceId)));
      properties.put("schema", format.getSchema().toString());
      PluginSpec pluginSpec = new PluginSpec("GCSFile", "source", properties);
      GCSSpec spec = new GCSSpec(pluginSpec);

      return new ServiceResponse<>(spec);
    });
  }

  private Set<String> getBucketWhitelist(Connection connection) {
    String whitelistStr = connection.getProperties().get("bucketWhitelist");
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
