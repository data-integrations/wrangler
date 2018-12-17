/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.proto.PluginSpec;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.s3.S3ConnectionSample;
import co.cask.wrangler.proto.s3.S3ObjectInfo;
import co.cask.wrangler.proto.s3.S3Spec;
import co.cask.wrangler.sampling.Bernoulli;
import co.cask.wrangler.sampling.Poisson;
import co.cask.wrangler.sampling.Reservoir;
import co.cask.wrangler.service.FileTypeDetector;
import co.cask.wrangler.service.common.AbstractWranglerService;
import co.cask.wrangler.service.common.Format;
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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
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

import static co.cask.wrangler.ServiceUtils.error;

/**
 * Service to explore S3 filesystem
 */
public class S3Service extends AbstractWranglerService {
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
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent(Charsets.UTF_8.name(), Connection.class);
      if (connection == null) {
        responder.sendError(400, "Connection information is missing from the request body.");
        return;
      }
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
                              @QueryParam("path") String path,
                              @QueryParam("limit") @DefaultValue("1000") int bucketLimit) {
    try {
      Connection[] connection = new Connection[1];
      getContext().execute(datasetContext -> connection[0] = store.get(connectionId));
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
        List<S3ObjectInfo> bucketInfo = new ArrayList<>(buckets.size());
        for (Bucket bucket : buckets) {
          bucketInfo.add(S3ObjectInfo.ofBucket(bucket));
        }
        ServiceResponse<S3ObjectInfo> response = new ServiceResponse<>(bucketInfo);
        responder.sendJson(response);
        return;
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
          objects.add(S3ObjectInfo.ofDir(dir));
        }
        for (S3ObjectSummary summary : result.getObjectSummaries()) {
          objects.add(S3ObjectInfo.ofObject(summary, detector));
        }
        listObjectsRequest.setMarker(result.getMarker());
      } while (result.isTruncated());

      ServiceResponse<S3ObjectInfo> response = new ServiceResponse<>(objects, limitExceeded);
      responder.sendJson(response);
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

      String header = request.getHeader(PropertyIds.CONTENT_TYPE);
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
                            @PathParam("bucket-name") String bucketName, @QueryParam("key") final String key,
                            @QueryParam("wid") String workspaceId) {
    try {
      Format format = Format.TEXT;
      if (workspaceId != null) {
        Map<String, String> config = ws.getProperties(workspaceId);
        String formatStr = config.getOrDefault(PropertyIds.FORMAT, Format.TEXT.name());
        format = Format.valueOf(formatStr);
      }
      Connection conn = store.get(connectionId);
      S3Configuration s3Configuration = new S3Configuration(conn);
      Map<String, String> properties = new HashMap<>();
      properties.put("format", format.name().toLowerCase());
      properties.put("accessID", s3Configuration.getAWSAccessKeyId());
      properties.put("accessKey", s3Configuration.getAWSSecretKey());
      properties.put("path", String.format("s3n://%s/%s", bucketName, key));
      properties.put("copyHeader", String.valueOf(shouldCopyHeader(workspaceId)));
      properties.put("schema", format.getSchema().toString());

      PluginSpec pluginSpec = new PluginSpec("S3", "source", properties);
      S3Spec spec = new S3Spec(pluginSpec);
      ServiceResponse<S3Spec> response = new ServiceResponse<>(spec);
      responder.sendJson(response);
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  private void loadSamplableFile(String connectionId, HttpServiceResponder responder,
                                 String scope, InputStream inputStream, S3Object s3Object,
                                 int lines, double fraction, String sampler) {
    SamplingMethod samplingMethod = SamplingMethod.fromString(sampler);
    if (sampler == null || sampler.isEmpty() || SamplingMethod.fromString(sampler) == null) {
      samplingMethod = SamplingMethod.FIRST;
    }

    try (BoundedLineInputStream blis = BoundedLineInputStream.iterator(inputStream, Charsets.UTF_8, lines)) {
      String name = s3Object.getKey();

      String file = String.format("%s:%s:%s", scope, s3Object.getBucketName(), s3Object.getKey());
      String identifier = ServiceUtils.generateMD5(file);
      String fileName = name.substring(name.lastIndexOf("/") + 1);
      ws.createWorkspaceMeta(identifier, scope, fileName);

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
      ws.writeProperties(identifier, properties);

      // Write rows to workspace.
      ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
      byte[] data = serDe.toByteArray(rows);
      ws.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL, DataType.RECORDS, data);

      // Preparing return response to include mandatory fields : id and name.
      S3ConnectionSample sampleInfo = new S3ConnectionSample(identifier, name, ConnectionType.S3.getType(),
                                                             samplingMethod.getMethod(), connectionId,
                                                             s3Object.getBucketName(), s3Object.getKey());
      ServiceResponse<S3ConnectionSample> response = new ServiceResponse<>(sampleInfo);
      responder.sendJson(response);
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  private void loadFile(String connectionId, HttpServiceResponder responder, InputStream inputStream,
                        S3Object s3Object) {
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
      ws.createWorkspaceMeta(identifier, fileName);

      stream = new BufferedInputStream(inputStream);
      byte[] bytes = new byte[(int) s3Object.getObjectMetadata().getContentLength() + 1];
      stream.read(bytes);

      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.ID, identifier);
      properties.put(PropertyIds.NAME, fileName);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.S3.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, connectionId);
      DataType dataType = getDataType(name);
      Format format = dataType == DataType.BINARY ? Format.BLOB : Format.TEXT;
      properties.put(PropertyIds.FORMAT, format.name());

      // S3 specific properties.
      properties.put("bucket-name", s3Object.getBucketName());
      properties.put("key", s3Object.getKey());
      ws.writeProperties(identifier, properties);
      ws.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL, getDataType(name), bytes);

      // Preparing return response to include mandatory fields : id and name.
      S3ConnectionSample sampleInfo = new S3ConnectionSample(identifier, name, ConnectionType.S3.getType(),
                                                             SamplingMethod.NONE.getMethod(), connectionId,
                                                             s3Object.getBucketName(), s3Object.getKey());
      ServiceResponse<S3ConnectionSample> response = new ServiceResponse<>(sampleInfo);
      responder.sendJson(response);
    } catch (Exception e) {
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
}
