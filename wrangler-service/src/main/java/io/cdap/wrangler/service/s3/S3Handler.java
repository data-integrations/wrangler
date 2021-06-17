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

package io.cdap.wrangler.service.s3;

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
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
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
import io.cdap.wrangler.proto.StatusCodeException;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.s3.S3ConnectionSample;
import io.cdap.wrangler.proto.s3.S3ObjectInfo;
import io.cdap.wrangler.proto.s3.S3Spec;
import io.cdap.wrangler.sampling.Bernoulli;
import io.cdap.wrangler.sampling.Poisson;
import io.cdap.wrangler.sampling.Reservoir;
import io.cdap.wrangler.service.FileTypeDetector;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.service.common.Format;
import io.cdap.wrangler.service.explorer.BoundedLineInputStream;
import io.cdap.wrangler.service.macro.ServiceMacroEvaluator;
import io.cdap.wrangler.utils.ObjectSerDe;

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
 * Service to explore S3 filesystem.
 */
@Deprecated
public class S3Handler extends AbstractWranglerHandler {
  private static final String PATH_FORMAT = "s3n://%s/%s";
  private static final String BUCKET_NAME = "bucket-name";
  private static final String KEY = "key";
  private static final String COLUMN_NAME = "body";
  private static final int FILE_SIZE = 10 * 1024 * 1024;
  private static final List<String> MACRO_FIELDS = ImmutableList.of("accessKeyId", "accessSecretKey");
  private static final FileTypeDetector detector = new FileTypeDetector();
  private final Map<String, ServiceMacroEvaluator> macroEvaluators = new HashMap<>();

  @POST
  @Path("/contexts/{context}/connections/s3/test")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void testS3Connection(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("context") String namespace) {
    respond(request, responder, () -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.S3);
      // creating a client doesn't test the connection, we will do list buckets so the connection is tested.
      initializeAndGetS3Client(connection, namespace, getContext()).listBuckets();
      return new ServiceResponse<Void>("Success");
    });
  }

  // creates s3 client and sets region and returns the initialized client
  private AmazonS3 initializeAndGetS3Client(ConnectionMeta connection, String namespace,
                                            SystemHttpServiceContext context) {
    Map<String, String> evaluateMacros = evaluateMacros(connection, context, namespace);
    evaluateMacros.put("region", connection.getProperties().get("region"));
    S3Configuration s3Configuration = new S3Configuration(evaluateMacros);
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
  @GET
  @Path("contexts/{context}/connections/{connection-id}/s3/explore")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void getS3BucketInfo(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace, @PathParam("connection-id") String connectionId,
                              @QueryParam("path") String path,
                              @QueryParam("limit") @DefaultValue("1000") int bucketLimit) {
    respond(request, responder, namespace, ns -> {
      try {
        Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId), ConnectionType.S3);
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

        AmazonS3 s3 = initializeAndGetS3Client(connection, namespace, getContext());
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

  /**
   * Reads s3 object into workspace
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("contexts/{context}/connections/{connection-id}/s3/buckets/{bucket-name}/read")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void loadObject(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace, @PathParam("connection-id") String connectionId,
                         @PathParam("bucket-name") String bucketName,
                         @QueryParam("key") String key, @QueryParam("lines") int lines,
                         @QueryParam("sampler") String sampler, @QueryParam("fraction") double fraction,
                         @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, ns -> {
      try {
        if (Strings.isNullOrEmpty(key)) {
          throw new BadRequestException("Required query param 'key' is missing in the input");
        }

        String header = request.getHeader(PropertyIds.CONTENT_TYPE);
        NamespacedId namespacedConnId = new NamespacedId(ns, connectionId);
        Connection connection = getValidatedConnection(namespacedConnId, ConnectionType.S3);
        AmazonS3 s3 = initializeAndGetS3Client(connection, namespace, getContext());
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        if (object == null) {
          throw new BadRequestException(
            String.format("S3 Object with key %s and bucket-name %s is not found", key, bucketName));
        }

        try (InputStream inputStream = object.getObjectContent()) {
          S3ConnectionSample sample;
          if (header != null && header.equalsIgnoreCase("text/plain")) {
            sample = loadSamplableFile(namespacedConnId, scope, inputStream, object, lines, fraction, sampler);
          } else {
            sample = loadFile(namespacedConnId, scope, inputStream, object);
          }
          return new ServiceResponse<>(sample);
        }
      } catch (AmazonS3Exception e) {
        throw new StatusCodeException(e.getMessage(), e, e.getStatusCode());
      }
    });
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param bucketName S3 object's bucket name
   * @param key S3 object's key
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/s3/buckets/{bucket-name}/specification")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace, @PathParam("connection-id") String connectionId,
                            @PathParam("bucket-name") String bucketName, @QueryParam("key") String key,
                            @QueryParam("wid") String workspaceId) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      ConnectionStore store = ConnectionStore.get(context);
      WorkspaceDataset ws = WorkspaceDataset.get(context);
      Format format = Format.TEXT;
      NamespacedId namespacedWorkspaceId = new NamespacedId(ns, workspaceId);
      if (workspaceId != null) {
        Map<String, String> config = ws.getWorkspace(namespacedWorkspaceId).getProperties();
        String formatStr = config.getOrDefault(PropertyIds.FORMAT, Format.TEXT.name());
        format = Format.valueOf(formatStr);
      }
      Connection conn = getValidatedConnection(store, new NamespacedId(ns, connectionId), ConnectionType.S3);
      S3Configuration s3Configuration = new S3Configuration(conn.getProperties());
      Map<String, String> properties = new HashMap<>();
      properties.put("format", format.name().toLowerCase());
      properties.put("accessID", s3Configuration.getAWSAccessKeyId());
      properties.put("accessKey", s3Configuration.getAWSSecretKey());
      properties.put("path", String.format("s3n://%s/%s", bucketName, key));
      properties.put("copyHeader", String.valueOf(shouldCopyHeader(ws, namespacedWorkspaceId)));
      properties.put("schema", format.getSchema().toString());

      PluginSpec pluginSpec = new PluginSpec("S3", "source", properties);
      S3Spec spec = new S3Spec(pluginSpec);
      return new ServiceResponse<>(spec);
    }));
  }

  public static Map<String, String> getConnectorProperties(Map<String, String> config) {
    Map<String, String> properties = new HashMap<>();
    S3Configuration s3Configuration = new S3Configuration(config);
    properties.put("accessID", s3Configuration.getAWSAccessKeyId());
    properties.put("accessKey", s3Configuration.getAWSSecretKey());
    properties.put("authenticationMethod", "Access Credentials");
    properties.put("region", s3Configuration.getRegion());
    return properties;
  }

  public static String getPath(Workspace workspace) {
    return String.format(PATH_FORMAT, workspace.getProperties().get(BUCKET_NAME), workspace.getProperties().get(KEY));
  }

  private S3ConnectionSample loadSamplableFile(NamespacedId connectionId, String scope, InputStream inputStream,
                                               S3Object s3Object, int lines, double fraction,
                                               String sampler) throws IOException {
    SamplingMethod samplingMethod;
    if (sampler == null || sampler.isEmpty() || SamplingMethod.fromString(sampler) == null) {
      samplingMethod = SamplingMethod.FIRST;
    } else {
      samplingMethod = SamplingMethod.fromString(sampler);
    }

    try (BoundedLineInputStream blis = BoundedLineInputStream.iterator(inputStream, Charsets.UTF_8, lines)) {
      String name = s3Object.getKey();

      String fileName = name.substring(name.lastIndexOf("/") + 1);
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.NAME, fileName);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, connectionId.getId());
      properties.put(BUCKET_NAME, s3Object.getBucketName());
      properties.put(KEY, s3Object.getKey());
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(fileName)
        .setScope(scope)
        .setProperties(properties)
        .build();
      String sampleId = TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        NamespacedId workspaceId = ws.createWorkspace(connectionId.getNamespace(), workspaceMeta);

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
        ws.updateWorkspaceData(workspaceId, DataType.RECORDS, data);
        return workspaceId.getId();
      });

      // Preparing return response to include mandatory fields : id and name.
      return new S3ConnectionSample(sampleId, name, ConnectionType.S3.getType(),
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
    String fileName = name.substring(name.lastIndexOf("/") + 1);

    byte[] bytes = new byte[(int) s3Object.getObjectMetadata().getContentLength() + 1];
    try (BufferedInputStream stream = new BufferedInputStream(inputStream)) {
      stream.read(bytes);
    }

    Map<String, String> properties = new HashMap<>();
    properties.put(PropertyIds.NAME, fileName);
    properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
    properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
    properties.put(PropertyIds.CONNECTION_ID, connectionId.getId());
    DataType dataType = getDataType(name);
    Format format = dataType == DataType.BINARY ? Format.BLOB : Format.TEXT;
    properties.put(PropertyIds.FORMAT, format.name());

    // S3 specific properties.
    properties.put(BUCKET_NAME, s3Object.getBucketName());
    properties.put(KEY, s3Object.getKey());
    WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(fileName)
      .setScope(scope)
      .setProperties(properties)
      .build();
    String sampleId = TransactionRunners.run(getContext(), context -> {
      WorkspaceDataset ws = WorkspaceDataset.get(context);
      NamespacedId workspaceId = ws.createWorkspace(connectionId.getNamespace(), workspaceMeta);
      ws.updateWorkspaceData(workspaceId, getDataType(name), bytes);
      return workspaceId.getId();
    });

    // Preparing return response to include mandatory fields : id and name.
    return new S3ConnectionSample(sampleId, name, ConnectionType.S3.getType(),
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

  /**
   * Evaluates all the MACRO_FIELDS.
   */
  private Map<String, String> evaluateMacros(ConnectionMeta connection, SystemHttpServiceContext context,
                                             String namespaceName) {
    Map<String, String> toEvaluate = new HashMap<>();
    for (String field : MACRO_FIELDS) {
      toEvaluate.put(field, connection.getProperties().get(field));
    }

    macroEvaluators.putIfAbsent(namespaceName, new ServiceMacroEvaluator(namespaceName, context));
    return context.evaluateMacros(namespaceName, toEvaluate, macroEvaluators.get(namespaceName));
  }
}
