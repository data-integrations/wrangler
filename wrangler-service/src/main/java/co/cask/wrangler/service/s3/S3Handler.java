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

package co.cask.wrangler.service.s3;

import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.ReadWrite;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
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
import co.cask.wrangler.proto.StatusCodeException;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionMeta;
import co.cask.wrangler.proto.connection.ConnectionType;
import co.cask.wrangler.proto.s3.S3ConnectionSample;
import co.cask.wrangler.proto.s3.S3ObjectInfo;
import co.cask.wrangler.proto.s3.S3Spec;
import co.cask.wrangler.sampling.Bernoulli;
import co.cask.wrangler.sampling.Poisson;
import co.cask.wrangler.sampling.Reservoir;
import co.cask.wrangler.service.FileTypeDetector;
import co.cask.wrangler.service.common.AbstractWranglerHandler;
import co.cask.wrangler.service.common.Format;
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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Service to explore S3 filesystem
 */
public class S3Handler extends AbstractWranglerHandler {
  private static final String COLUMN_NAME = "body";
  private static final int FILE_SIZE = 10 * 1024 * 1024;

  private static final FileTypeDetector detector = new FileTypeDetector();

  /**
   * Tests S3 Connection.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("/connections/s3/test")
  public void testS3Connection(HttpServiceRequest request, HttpServiceResponder responder) {
    respond(request, responder, () -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.S3);
      // creating a client doesn't test the connection, we will do list buckets so the connection is tested.
      intializeAndGetS3Client(connection).listBuckets();
      // creating a client doesn't test the connection, we will do list buckets so the connection is tested.
      intializeAndGetS3Client(connection).listBuckets();
      return new ServiceResponse<Void>("Success");
    });
  }

  private void validateConnection(String connectionId, Connection connection) {
    if (connection == null) {
      throw new BadRequestException("Unable to find connection in store for the connection id - " + connectionId);
    }
    if (ConnectionType.S3 != connection.getType()) {
      throw new BadRequestException("Invalid connection type set, this endpoint only accepts S3 connection type");
    }
  }

  // creates s3 client and sets region and returns the initialized client
  private AmazonS3 intializeAndGetS3Client(ConnectionMeta connection) {
    S3Configuration s3Configuration = new S3Configuration(connection);
    AmazonS3 s3 = new AmazonS3Client(s3Configuration);
    Region region = Region.getRegion(Regions.fromName(s3Configuration.getRegion()));
    s3.setRegion(region);
    return s3;
  }

  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @ReadOnly
  @GET
  @Path("/connections/{connection-id}/s3/explore")
  public void getS3BucketInfo(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("connection-id") String connectionId,
                              @QueryParam("path") String path,
                              @QueryParam("limit") @DefaultValue("1000") int bucketLimit) {
    getS3BucketInfo(request, responder, getContext().getNamespace(), connectionId, path, bucketLimit);
  }

  /**
   * Lists S3 bucket's contents for the given prefix path.
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @ReadOnly
  @GET
  @Path("contexts/{context}/connections/{connection-id}/s3/explore")
  public void getS3BucketInfo(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace, @PathParam("connection-id") String connectionId,
                              @QueryParam("path") String path,
                              @QueryParam("limit") @DefaultValue("1000") int bucketLimit) {
    respond(request, responder, namespace, () -> {
      try {
        Connection connection = getValidatedConnection(new NamespacedId(namespace, connectionId), ConnectionType.S3);
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

        AmazonS3 s3 = intializeAndGetS3Client(connection);
        if (bucketName.isEmpty() && prefix == null) {
          List<Bucket> buckets = s3.listBuckets();
          List<S3ObjectInfo> bucketInfo = new ArrayList<>(buckets.size());
          for (Bucket bucket : buckets) {
            bucketInfo.add(fromBucket(bucket));
          }
          return new ServiceResponse<>(bucketInfo);
        }

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucketName);
        if (prefix != null) {
          listObjectsRequest.setPrefix(prefix);
        }
        listObjectsRequest.setDelimiter("/");
        ObjectListing result;
        List<S3ObjectInfo> objects = new ArrayList<>();
        // TODO: Remove this once CDAP-14446 is fixed.
        boolean limitExceeded = false;
        do {
          if (objects.size() >= bucketLimit) {
            limitExceeded = true;
            break;
          }
          result = s3.listObjects(listObjectsRequest);
          for (String dir : result.getCommonPrefixes()) {
            if (dir.equalsIgnoreCase("/")) {
              continue;
            }
            objects.add(fromDir(dir));
          }
          for (S3ObjectSummary summary : result.getObjectSummaries()) {
            objects.add(fromObject(summary, detector));
          }
          listObjectsRequest.setMarker(result.getMarker());
        } while (result.isTruncated());

        return new ServiceResponse<>(objects, limitExceeded);
      } catch (AmazonS3Exception e) {
        throw new StatusCodeException(e.getMessage(), e, e.getStatusCode());
      }
    });
  }

  @POST
  @ReadWrite
  @Path("/connections/{connection-id}/s3/buckets/{bucket-name}/read")
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("connection-id") String connectionId,
                         @PathParam("bucket-name") String bucketName,
                         @QueryParam("key") String key, @QueryParam("lines") int lines,
                         @QueryParam("sampler") String sampler, @QueryParam("fraction") double fraction,
                         @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    loadObject(request, responder, getContext().getNamespace(), connectionId, bucketName, key, lines, sampler,
               fraction, scope);
  }

  /**
   * Reads s3 object into workspace
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @ReadWrite
  @Path("contexts/{context}/connections/{connection-id}/s3/buckets/{bucket-name}/read")
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace, @PathParam("connection-id") String connectionId,
                         @PathParam("bucket-name") String bucketName,
                         @QueryParam("key") String key, @QueryParam("lines") int lines,
                         @QueryParam("sampler") String sampler, @QueryParam("fraction") double fraction,
                         @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, () -> {
      try {
        if (Strings.isNullOrEmpty(key)) {
          throw new BadRequestException("Required query param 'key' is missing in the input");
        }

        String header = request.getHeader(PropertyIds.CONTENT_TYPE);
        Connection connection = store.get(new NamespacedId(namespace, connectionId));
        validateConnection(connectionId, connection);
        AmazonS3 s3 = intializeAndGetS3Client(connection);
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        if (object == null) {
          throw new BadRequestException(
            String.format("S3 Object with key %s and bucket-name %s is not found", key, bucketName));
        }

        try (InputStream inputStream = object.getObjectContent()) {
          S3ConnectionSample sample;
          if (header != null && header.equalsIgnoreCase("text/plain")) {
            sample = loadSamplableFile(connection.getId(), scope, inputStream, object, lines, fraction, sampler);
          } else {
            sample = loadFile(connection.getId(), scope, inputStream, object);
          }
          return new ServiceResponse<>(sample);
        }
      } catch (AmazonS3Exception e) {
        throw new StatusCodeException(e.getMessage(), e, e.getStatusCode());
      }
    });
  }

  @Path("/connections/{connection-id}/s3/buckets/{bucket-name}/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("connection-id") String connectionId,
                            @PathParam("bucket-name") String bucketName, @QueryParam("key") String key,
                            @QueryParam("wid") String workspaceId) {
    specification(request, responder, getContext().getNamespace(), connectionId, bucketName, key, workspaceId);
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param bucketName S3 object's bucket name
   * @param key S3 object's key
   */
  @Path("contexts/{context}/connections/{connection-id}/s3/buckets/{bucket-name}/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace, @PathParam("connection-id") String connectionId,
                            @PathParam("bucket-name") String bucketName, @QueryParam("key") String key,
                            @QueryParam("wid") String workspaceId) {
    respond(request, responder, namespace, () -> {
      Format format = Format.TEXT;
      NamespacedId namespacedWorkspaceId = new NamespacedId(namespace, workspaceId);
      if (workspaceId != null) {
        Map<String, String> config = ws.getWorkspace(namespacedWorkspaceId).getProperties();
        String formatStr = config.getOrDefault(PropertyIds.FORMAT, Format.TEXT.name());
        format = Format.valueOf(formatStr);
      }
      Connection conn = store.get(new NamespacedId(namespace, connectionId));
      S3Configuration s3Configuration = new S3Configuration(conn);
      Map<String, String> properties = new HashMap<>();
      properties.put("format", format.name().toLowerCase());
      properties.put("accessID", s3Configuration.getAWSAccessKeyId());
      properties.put("accessKey", s3Configuration.getAWSSecretKey());
      properties.put("path", String.format("s3n://%s/%s", bucketName, key));
      properties.put("copyHeader", String.valueOf(shouldCopyHeader(namespacedWorkspaceId)));
      properties.put("schema", format.getSchema().toString());

      PluginSpec pluginSpec = new PluginSpec("S3", "source", properties);
      S3Spec spec = new S3Spec(pluginSpec);
      return new ServiceResponse<>(spec);
    });
  }

  private S3ConnectionSample loadSamplableFile(NamespacedId connectionId, String scope, InputStream inputStream,
                                               S3Object s3Object, int lines, double fraction,
                                               String sampler) throws IOException {
    SamplingMethod samplingMethod = SamplingMethod.fromString(sampler);
    if (sampler == null || sampler.isEmpty() || SamplingMethod.fromString(sampler) == null) {
      samplingMethod = SamplingMethod.FIRST;
    }

    try (BoundedLineInputStream blis = BoundedLineInputStream.iterator(inputStream, Charsets.UTF_8, lines)) {
      String name = s3Object.getKey();

      String file = String.format("%s:%s:%s", scope, s3Object.getBucketName(), s3Object.getKey());
      String identifier = ServiceUtils.generateMD5(file);
      String fileName = name.substring(name.lastIndexOf("/") + 1);
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.ID, identifier);
      properties.put(PropertyIds.NAME, fileName);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, connectionId.getId());
      properties.put("bucket-name", s3Object.getBucketName());
      properties.put("key", s3Object.getKey());
      NamespacedId namespacedWorkspaceId = new NamespacedId(connectionId.getNamespace(), identifier);
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(namespacedWorkspaceId, fileName)
        .setScope(scope)
        .setProperties(properties)
        .build();
      ws.writeWorkspaceMeta(workspaceMeta);

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
      while (it.hasNext()) {
        rows.add(new Row(COLUMN_NAME, it.next()));
      }

      // Write rows to workspace.
      ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
      byte[] data = serDe.toByteArray(rows);
      ws.updateWorkspaceData(namespacedWorkspaceId, DataType.RECORDS, data);

      // Preparing return response to include mandatory fields : id and name.
      return new S3ConnectionSample(namespacedWorkspaceId.getId(), name, ConnectionType.S3.getType(),
                                    samplingMethod.getMethod(), connectionId.getId(),
                                    s3Object.getBucketName(), s3Object.getKey());
    }
  }

  private S3ConnectionSample loadFile(NamespacedId connectionId, String scope,
                                      InputStream inputStream, S3Object s3Object) throws IOException {
    if (s3Object.getObjectMetadata().getContentLength() > FILE_SIZE) {
      throw new BadRequestException("Files greater than 10MB are not supported.");
    }

    // Creates workspace.
    String name = s3Object.getKey();

    String file = String.format("%s:%s", s3Object.getBucketName(), s3Object.getKey());
    String identifier = ServiceUtils.generateMD5(file);
    String fileName = name.substring(name.lastIndexOf("/") + 1);

    byte[] bytes = new byte[(int) s3Object.getObjectMetadata().getContentLength() + 1];
    try (BufferedInputStream stream = new BufferedInputStream(inputStream)) {
      stream.read(bytes);
    }

    Map<String, String> properties = new HashMap<>();
    properties.put(PropertyIds.ID, identifier);
    properties.put(PropertyIds.NAME, fileName);
    properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
    properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
    properties.put(PropertyIds.CONNECTION_ID, connectionId.getId());
    DataType dataType = getDataType(name);
    Format format = dataType == DataType.BINARY ? Format.BLOB : Format.TEXT;
    properties.put(PropertyIds.FORMAT, format.name());

    // S3 specific properties.
    properties.put("bucket-name", s3Object.getBucketName());
    properties.put("key", s3Object.getKey());
    NamespacedId namespacedWorkspaceId = new NamespacedId(connectionId.getNamespace(), identifier);
    WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(namespacedWorkspaceId, fileName)
      .setScope(scope)
      .setProperties(properties)
      .build();
    ws.writeWorkspaceMeta(workspaceMeta);
    ws.updateWorkspaceData(namespacedWorkspaceId, getDataType(name), bytes);

    // Preparing return response to include mandatory fields : id and name.
    return new S3ConnectionSample(namespacedWorkspaceId.getId(), name, ConnectionType.S3.getType(),
                                  SamplingMethod.NONE.getMethod(), connectionId.getId(),
                                  s3Object.getBucketName(), s3Object.getKey());
  }

  /**
   * get data type from the file type.
   *
   * @param fileName the file name
   * @return DataType
   */
  private DataType getDataType(String fileName) {
    // detect fileType from fileName
    String fileType = detector.detectFileType(fileName);
    DataType dataType = DataType.fromString(fileType);
    return dataType == null ? DataType.BINARY : dataType;
  }

  private static S3ObjectInfo fromBucket(Bucket bucket) {
    return S3ObjectInfo.builder(bucket.getName(), "bucket")
      .setCreated(bucket.getCreationDate().getTime())
      .setOwner(bucket.getOwner().getDisplayName())
      .setIsDirectory(true)
      .build();
  }

  public static S3ObjectInfo fromDir(String dir) {
    String[] parts = dir.split("/");
    String name = dir;
    if (parts.length > 1) {
      name = parts[parts.length - 1];
    }
    return S3ObjectInfo.builder(name, "directory").setPath(dir).setIsDirectory(true).build();
  }

  public static S3ObjectInfo fromObject(S3ObjectSummary summary, FileTypeDetector detector) {
    int idx = summary.getKey().lastIndexOf("/");
    String name = summary.getKey();
    if (idx != -1) {
      name = name.substring(idx + 1);
    }
    String type = detector.detectFileType(name);
    boolean canWrangle = detector.isWrangleable(type);
    return S3ObjectInfo.builder(name, type)
      .setPath(summary.getKey())
      .setOwner(summary.getOwner().getDisplayName())
      .setStorageClass(summary.getStorageClass())
      .setLastModified(summary.getLastModified().getTime())
      .setSize(summary.getSize())
      .setIsDirectory(false)
      .setCanWrangle(canWrangle)
      .build();
  }
}
