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

package io.cdap.wrangler.service.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Splitter;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.BytesDecoder;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.RequestExtractor;
import io.cdap.wrangler.SamplingMethod;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.dataset.connections.ConnectionStore;
import io.cdap.wrangler.dataset.workspace.DataType;
import io.cdap.wrangler.dataset.workspace.Workspace;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.dataset.workspace.WorkspaceMeta;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.PluginSpec;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.gcs.GCSBucketInfo;
import io.cdap.wrangler.proto.gcs.GCSConnectionSample;
import io.cdap.wrangler.proto.gcs.GCSObjectInfo;
import io.cdap.wrangler.proto.gcs.GCSSpec;
import io.cdap.wrangler.service.FileTypeDetector;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.service.common.Format;
import io.cdap.wrangler.service.gcp.GCPUtils;
import io.cdap.wrangler.utils.ObjectSerDe;
import io.cdap.wrangler.utils.ReferenceNames;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Service to explore <code>GCS</code> filesystem
 */
@Deprecated
public class GCSHandler extends AbstractWranglerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(GCSHandler.class);
  private static final String MAX_SAMPLE_ROWS = "wrangler.gcs.sampling.max.rows";
  static final long FILE_SIZE = 10 * 1024 * 1024;
  private FileTypeDetector detector;
  private int maxSampleRows;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    Security.addProvider(new BouncyCastleProvider());
    this.detector = new FileTypeDetector();
    this.maxSampleRows = Integer.parseInt(context.getRuntimeArguments().getOrDefault(MAX_SAMPLE_ROWS, "5000"));
  }

  @POST
  @Path("/contexts/{context}/connections/gcs/test")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void testGCSConnection(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("context") String namespace) {
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

  /**
   * Lists GCS bucket's contents for the given prefix path.
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/gcs/explore")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void exploreGCS(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace,
                         @PathParam("connection-id") String connectionId,
                         @QueryParam("path") String path,
                         @QueryParam("limit") @DefaultValue("1000") int objectLimit) {
    respond(request, responder, namespace, ns -> {
      String bucketName = "";
      String prefix = null;

      Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId), ConnectionType.GCS);

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

  /**
   * Reads GCS object into workspace.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/gcs/buckets/{bucket}/read")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace,
                         @PathParam("connection-id") String connectionId,
                         @PathParam("bucket") String bucket,
                         @QueryParam("blob") String blobPath,
                         @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, ns -> {
      String contentType = request.getHeader(PropertyIds.CONTENT_TYPE);

      if (blobPath == null || blobPath.isEmpty()) {
        throw new BadRequestException("Required query param 'path' is missing in the input");
      }

      Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId), ConnectionType.GCS);

      Map<String, String> properties = new HashMap<>();
      Storage storage = GCPUtils.getStorageService(connection);
      Blob blob = storage.get(BlobId.of(bucket, blobPath));
      if (blob == null) {
        throw new BadRequestException(String.format("Bucket '%s', Path '%s' is not valid.", bucket, blobPath));
      }

      String blobName = blob.getName();
      File file = new File(blobName);

      // Set all properties and write to workspace.
      properties.put(PropertyIds.FILE_NAME, file.getCanonicalPath());
      properties.put(PropertyIds.URI, String.format("gs://%s/%s", bucket, blobPath));
      properties.put(PropertyIds.FILE_PATH, blobPath);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.GCS.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, connectionId);
      properties.put("bucket", bucket);
      if (blob.isDirectory()) {
        throw new BadRequestException(String.format("Path '%s' is not a file.", blob.getName()));
      }

      String sampleId = TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        boolean shouldTruncate = blob.getSize() > FILE_SIZE;
        byte[] bytes = readGCSFile(blob, (int) (shouldTruncate ? FILE_SIZE : blob.getSize()));
        DataType dataType;
        byte[] result = bytes;

        String encoding = BytesDecoder.guessEncoding(bytes);
        if (contentType.equalsIgnoreCase("text/plain")
          && (encoding.equalsIgnoreCase("utf-8") || encoding.equalsIgnoreCase("ascii"))) {
          String data = new String(bytes, encoding);

          Iterator<String> lineIter = Splitter.on(Pattern.compile("\r\n|\r|\n")).split(data).iterator();

          List<Row> rows = new ArrayList<>();
          int numLines = 0;
          while (lineIter.hasNext() && numLines < maxSampleRows) {
            numLines++;
            String line = lineIter.next();
            // CDAP-17029 - ignore the last line if it is empty
            if (line.isEmpty() && !lineIter.hasNext()) {
              break;
            }
            rows.add(new Row("body", line));
          }

          // if the content was truncated and we didn't reach the maximum number of sample rows,
          // ignore the last line because it's probably not complete.
          if (shouldTruncate && !lineIter.hasNext()) {
            rows.remove(rows.size() - 1);
          }

          if (shouldTruncate && rows.size() == 0) {
            throw new BadRequestException("A single line of text file is larger than "
                                            + FILE_SIZE + " bytes, unable to process");
          }

          ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
          result = serDe.toByteArray(rows);
          dataType = DataType.RECORDS;
          properties.put(PropertyIds.FORMAT, Format.TEXT.name());
        } else if (contentType.equalsIgnoreCase("application/json")) {
          dataType = DataType.TEXT;
          properties.put(PropertyIds.FORMAT, Format.TEXT.name());
        } else if (contentType.equalsIgnoreCase("application/xml")) {
          dataType = DataType.TEXT;
          properties.put(PropertyIds.FORMAT, Format.BLOB.name());
        } else {
          dataType = DataType.BINARY;
          properties.put(PropertyIds.FORMAT, Format.BLOB.name());
        }

        WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(file.getName())
                                        .setScope(scope)
                                        .setProperties(properties)
                                        .build();
        NamespacedId workspaceId = ws.createWorkspace(ns, workspaceMeta);
        ws.updateWorkspaceData(workspaceId, dataType, result);
        return workspaceId.getId();
      });

      // Preparing return response to include mandatory fields : id and name.
      GCSConnectionSample connectionSample =
        new GCSConnectionSample(sampleId, file.getName(), ConnectionType.GCS.getType(), SamplingMethod.NONE.getMethod(),
                                connectionId, String.format("gs://%s/%s", bucket, blobPath), blobPath, blobName,
                                bucket);

      return new ServiceResponse<>(connectionSample);
    });
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/gcs/specification")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace, @PathParam("connection-id") String connectionId,
                            @QueryParam("wid") String workspaceId) {
    respond(request, responder, namespace, ns -> {
      if (workspaceId == null) {
        throw new BadRequestException("Workspace ID must be passed as query parameter 'wid'.");
      }

      return TransactionRunners.run(getContext(), context -> {
        ConnectionStore store = ConnectionStore.get(context);
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        Connection connection = getValidatedConnection(store, new NamespacedId(ns, connectionId),
                                                       ConnectionType.GCS);

        NamespacedId namespacedIdWorkspaceId = new NamespacedId(ns, workspaceId);
        Map<String, String> config = ws.getWorkspace(namespacedIdWorkspaceId).getProperties();
        String formatStr = config.getOrDefault(PropertyIds.FORMAT, Format.TEXT.name());
        Format format = Format.valueOf(formatStr);
        String uri = config.get(PropertyIds.URI);
        String[] parts = uri.split("/");
        String filename = parts[parts.length - 1];
        String externalFileName = new StringJoiner(".").add(config.get("bucket")).add(filename).toString();

        Map<String, String> properties = new HashMap<>();
        properties.put("format", format.name().toLowerCase());
        properties.put("referenceName", ReferenceNames.cleanseReferenceName(externalFileName));
        properties.put("serviceFilePath", connection.getProperties().get(GCPUtils.SERVICE_ACCOUNT_KEYFILE));
        properties.put("project", GCPUtils.getProjectId(connection));
        properties.put("path", uri);
        properties.put("recursive", "false");
        properties.put("filenameOnly", "false");
        properties.put("copyHeader", String.valueOf(shouldCopyHeader(ws, namespacedIdWorkspaceId)));
        properties.put("schema", format.getSchema().toString());
        PluginSpec pluginSpec = new PluginSpec("GCSFile", "source", properties);
        GCSSpec spec = new GCSSpec(pluginSpec);
        return new ServiceResponse<>(spec);
      });

    });
  }

  public static Map<String, String> getConnectorProperties(Map<String, String> config) {
    Map<String, String> properties = new HashMap<>();
    properties.put("serviceAccountType", "filePath");
    properties.put("serviceFilePath", config.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE));
    properties.put("project", config.get(GCPUtils.PROJECT_ID));
    properties.values().removeIf(Objects::isNull);
    return properties;
  }

  public static String getPath(Workspace workspace) {
    return workspace.getProperties().get(PropertyIds.URI);
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
