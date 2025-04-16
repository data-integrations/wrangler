/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.wrangler.service.adls;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import io.cdap.cdap.api.annotation.ReadWrite;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.RequestExtractor;
import io.cdap.wrangler.SamplingMethod;
import io.cdap.wrangler.ServiceUtils;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.dataset.connections.ConnectionStore;
import io.cdap.wrangler.dataset.workspace.DataType;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.dataset.workspace.WorkspaceMeta;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.NotFoundException;
import io.cdap.wrangler.proto.PluginSpec;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.StatusCodeException;
import io.cdap.wrangler.proto.adls.ADLSConnectionSample;
import io.cdap.wrangler.proto.adls.ADLSDirectoryEntryInfo;
import io.cdap.wrangler.proto.adls.FileQueryDetails;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.sampling.Bernoulli;
import io.cdap.wrangler.sampling.Poisson;
import io.cdap.wrangler.sampling.Reservoir;
import io.cdap.wrangler.service.FileTypeDetector;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.service.common.Format;
import io.cdap.wrangler.service.explorer.BoundedLineInputStream;
import io.cdap.wrangler.utils.ObjectSerDe;
import io.cdap.wrangler.utils.ReferenceNames;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;


/**
 * Service to explore ADLS Gen1 filesystem.
 */
@Deprecated
public class ADLSHandler extends AbstractWranglerHandler {
  private static final String COLUMN_NAME = "body";
  private static final int FILE_SIZE = 10 * 1024 * 1024;
  private static final FileTypeDetector detector = new FileTypeDetector();

  /**
   * Create an ADLS client using connection details from the HTTP request.
   *
   * @param connection connection details from the HTTP request.
   * @return ADLStoreClient
   */
  private ADLStoreClient initializeAndGetADLSClient(ConnectionMeta connection) {
    ADLSConfiguration configuration = new ADLSConfiguration(connection.getProperties());
    String authTokenEndpoint = configuration.getEndpointURL();
    String clientId = configuration.getADLSClientID();
    String clientKey = configuration.getClientKey();
    String accountFQDN = configuration.getAccountFQDN();
    AccessTokenProvider provider = new ClientCredsTokenProvider(authTokenEndpoint, clientId, clientKey);
    return ADLStoreClient.createClient(accountFQDN, provider);
  }

  /**
   * Tests ADLS Connection.
   *
   * @param request   HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("contexts/{context}/connections/adls/test")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void testADLSConnection(HttpServiceRequest request, HttpServiceResponder responder,
                                 @PathParam("context") String namespace) {
    // Extract the body of the request and transform it to the Connection object
    respond(request, responder, () -> {
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.ADLS);
      ConnectionType connectionType = connection.getType();
      if (connectionType != ConnectionType.ADLS) {
        return new ServiceResponse<Void>(String.format("Invalid connection type %s set, expected " +
                        "'ADLS' connection type.", connectionType.getType()));
      }
      ADLStoreClient client = initializeAndGetADLSClient(connection);
      String output = testConnection(client);
      return new ServiceResponse<Void>(output);
    });
  }

  private String testConnection(ADLStoreClient client) throws IOException {
    try {
      client.enumerateDirectory("/");
    } catch (IOException e) {
      throw new IOException("Connection Failed, please check given credentials : " + e.getMessage());
    }
    return "Success";
  }

  /**
   * Lists ADLS directory's contents for the given prefix path.
   *
   * @param request   HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/adls/explore")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listADLSDirectory(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("context") String namespace,
                                @PathParam("connection-id") String connectionId,
                                @Nullable @QueryParam("path") String path) {
    respond(request, responder, namespace, ns -> {
      try {
        Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId), ConnectionType.ADLS);
        String defaultPath = "/";
        ADLStoreClient adlStoreClient = initializeAndGetADLSClient(connection);
        List<ADLSDirectoryEntryInfo> adlsDirectoryEntryInfos;
        if (path == null || path.equals("")) {
          adlsDirectoryEntryInfos = initClientReturnResponse(adlStoreClient, defaultPath);
        } else {
          adlsDirectoryEntryInfos = initClientReturnResponse(adlStoreClient, path);
        }
        return new ServiceResponse<>(adlsDirectoryEntryInfos);
      } catch (ADLException e) {
        throw new StatusCodeException(e.getMessage(), e, e.httpResponseCode);
      }
    });
  }

  private List<ADLSDirectoryEntryInfo> initClientReturnResponse(ADLStoreClient client, String path)
          throws IOException {
    if (!client.checkExists(path)) {
      throw new NotFoundException(String.format("Given path doesn't exist: %s", path));
    }
    List<DirectoryEntry> list = client.enumerateDirectory(path);
    ADLSDirectoryEntryInfo info;
    List<ADLSDirectoryEntryInfo> directoryEntryInfos = new ArrayList<>();
    for (DirectoryEntry entry : list) {

      if (entry.type.equals(DirectoryEntryType.DIRECTORY)) {
        info = ADLSDirectoryEntryInfo.builder(entry.name, entry.type.toString())
                .setPath(entry.fullName)
                .setDisplaySize(entry.length)
                .setGroup(entry.group)
                .setUser(entry.user)
                .setPermission(entry.permission)
                .setLastModified(entry.lastModifiedTime.toString())
                .setIsDirectory(true)
                .build();
      } else {
        String type = detector.detectFileType(entry.name);
        boolean canWrangle = detector.isWrangleable(type);
        info = ADLSDirectoryEntryInfo.builder(entry.name, entry.type.toString())
                .setPath(entry.fullName)
                .setDisplaySize(entry.length)
                .setGroup(entry.group)
                .setUser(entry.user)
                .setPermission(entry.permission)
                .setLastModified(entry.lastModifiedTime.toString())
                .setIsDirectory(false)
                .setCanWrangle(canWrangle)
                .build();
      }
      directoryEntryInfos.add(info);
    }
    return directoryEntryInfos;
  }

  /**
   * Reads ADLS file into workspace
   *
   * @param request   HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @ReadWrite
  @Path("contexts/{context}/connections/{connection-id}/adls/read")
  public void loadADLSFile(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace,
                           @PathParam("connection-id") String connectionId,
                           @QueryParam("path") String filePath, @QueryParam("lines") int lines,
                           @QueryParam("sampler") String sampler, @QueryParam("fraction") double fraction,
                           @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, ns -> {
      try {
        if (Strings.isNullOrEmpty(connectionId)) {
          throw new BadRequestException("Required path param 'connection-id' is missing in the input");
        }
        if (lines == 0) {
          throw new BadRequestException("The maximum number of sampled lines cannot be zero");
        }
        String header = request.getHeader(PropertyIds.CONTENT_TYPE);
        NamespacedId namespacedConnId = new NamespacedId(ns, connectionId);
        Connection connection = getValidatedConnection(namespacedConnId, ConnectionType.ADLS);
        FileQueryDetails fileQueryDetails = new FileQueryDetails(header, filePath, lines, sampler,
                fraction, scope);
        ADLSConnectionSample sample = fetchFileFromClient(connection, fileQueryDetails, namespacedConnId);
        return new ServiceResponse<>(sample);
      } catch (ADLException e) {
        throw new StatusCodeException(e.getMessage(), e, e.httpResponseCode);
      }
    });
  }

  /**
   * A method to fetch a file from an ADLS client
   *
   * @param connection Connection object
   * @param fileQueryDetails
   * @param namespaceID
   * @throws IOException
   */
  private ADLSConnectionSample fetchFileFromClient(Connection connection, FileQueryDetails fileQueryDetails,
                                                   NamespacedId namespaceID) throws IOException {
    ADLStoreClient client = initializeAndGetADLSClient(connection);
    DirectoryEntry file = getFileFromClient(client, fileQueryDetails.getFilePath());
    try (InputStream inputStream = clientInputStream(client, fileQueryDetails)) {
      if ("text/plain".equalsIgnoreCase(fileQueryDetails.getHeader())) {
        return loadSamplableFile(namespaceID, fileQueryDetails.getScope(), inputStream, file,
                fileQueryDetails.getLines(), fileQueryDetails.getFraction(), fileQueryDetails.getSampler());
      } else {
        return loadFile(namespaceID, fileQueryDetails.getScope(), inputStream, file);
      }
    } catch (ADLException e) {
      throw new StatusCodeException(e.getMessage(), e, e.httpResponseCode);
    }
  }

  private InputStream clientInputStream(ADLStoreClient client, FileQueryDetails fileQueryDetails)
          throws IOException {
    DirectoryEntry file = client.getDirectoryEntry(fileQueryDetails.getFilePath());
    return client.getReadStream(file.fullName);
  }

  private DirectoryEntry getFileFromClient(ADLStoreClient client, String path) throws IOException {
    return client.getDirectoryEntry(path);
  }

  private ADLSConnectionSample loadSamplableFile(NamespacedId connectionId,
                                                 String scope, InputStream inputStream, DirectoryEntry fileEntry,
                                                 int lines, double fraction, String sampler) throws IOException {
    SamplingMethod samplingMethod = SamplingMethod.fromString(sampler);

    if (sampler == null || sampler.isEmpty() || SamplingMethod.fromString(sampler) == null) {
      samplingMethod = SamplingMethod.FIRST;
    }
    final SamplingMethod samplingMethod1 = samplingMethod;
    try (BoundedLineInputStream blis = BoundedLineInputStream.iterator(inputStream, Charsets.UTF_8, lines)) {
      String name = fileEntry.name;
      String file = String.format("%s:%s", scope, fileEntry.name);
      String fileName = fileEntry.fullName;
      String identifier = ServiceUtils.generateMD5(file);
      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.FILE_PATH, fileEntry.fullName);
      properties.put(PropertyIds.NAME, name);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.ADLS.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, connectionId.getId());

      NamespacedId namespacedWorkspaceId = new NamespacedId(connectionId.getNamespace(), identifier);
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(fileName)
              .setScope(scope)
              .setProperties(properties)
              .build();
      TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        ws.writeWorkspaceMeta(namespacedWorkspaceId, workspaceMeta);

        // Iterate through lines to extract only 'limit' random lines.
        // Depending on the type, the sampling of the input is performed.
        List<Row> rows = new ArrayList<>();
        Iterator<String> it = blis;
        if (samplingMethod1 == SamplingMethod.POISSON) {
          it = new Poisson<String>(fraction).sample(blis);
        } else if (samplingMethod1 == SamplingMethod.BERNOULLI) {
          it = new Bernoulli<String>(fraction).sample(blis);
        } else if (samplingMethod1 == SamplingMethod.RESERVOIR) {
          it = new Reservoir<String>(lines).sample(blis);
        }
        while (it.hasNext()) {
          rows.add(new Row(COLUMN_NAME, it.next()));
        }

        // Write rows to workspace.
        ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
        byte[] data = serDe.toByteArray(rows);
        ws.updateWorkspaceData(namespacedWorkspaceId, DataType.RECORDS, data);
      });

      // Preparing return response to include mandatory fields : id and name.
      return new ADLSConnectionSample(namespacedWorkspaceId.getId(), name, ConnectionType.ADLS.getType(),
              samplingMethod.getMethod(), connectionId.getId());
    }
  }

  private ADLSConnectionSample loadFile(NamespacedId connectionId, String scope, InputStream inputStream,
                                        DirectoryEntry fileEntry) throws IOException {
    if (fileEntry.length > FILE_SIZE) {
      throw new BadRequestException("Files greater than 10MB are not supported.");
    }

    // Creates workspace.
    String name = fileEntry.name;

    String file = String.format("%s:%s", name, fileEntry.fullName);
    String identifier = ServiceUtils.generateMD5(file);
    String fileName = fileEntry.fullName;

    byte[] bytes = new byte[(int) fileEntry.length + 1];
    try (BufferedInputStream stream = new BufferedInputStream(inputStream)) {
      stream.read(bytes);
    }

    // Set all properties and write to workspace.
    Map<String, String> properties = new HashMap<>();
    properties.put(PropertyIds.NAME, name);
    properties.put(PropertyIds.FILE_PATH, fileEntry.fullName);
    properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.ADLS.getType());
    properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
    properties.put(PropertyIds.CONNECTION_ID, connectionId.getId());
    DataType dataType = getDataType(name);
    Format format = dataType == DataType.BINARY ? Format.BLOB : Format.TEXT;
    properties.put(PropertyIds.FORMAT, format.name());

    NamespacedId namespacedWorkspaceId = new NamespacedId(connectionId.getNamespace(), identifier);
    WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(fileName)
            .setScope(scope)
            .setProperties(properties)
            .build();
    TransactionRunners.run(getContext(), context -> {
      WorkspaceDataset ws = WorkspaceDataset.get(context);
      ws.writeWorkspaceMeta(namespacedWorkspaceId, workspaceMeta);
      ws.updateWorkspaceData(namespacedWorkspaceId, getDataType(name), bytes);
    });

    return new ADLSConnectionSample(namespacedWorkspaceId.getId(), name, ConnectionType.ADLS.getType(),
                                    SamplingMethod.NONE.getMethod(), connectionId.getId());
  }

  /**
   * Specification for the source.
   *
   * @param request   HTTP request handler.
   * @param responder HTTP response handler.
   */
  @Path("contexts/{context}/connections/{connection-id}/adls/specification")
  @GET
  public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                            @PathParam("context") String namespace,
                            @PathParam("connection-id") String connectionId,
                            @QueryParam("path") String path,
                            @QueryParam("wid") String workspaceId) {
    respond(request, responder, namespace, ns -> TransactionRunners.run(getContext(), context -> {
      ConnectionStore store = ConnectionStore.get(context);
      WorkspaceDataset ws = WorkspaceDataset.get(context);
      Format format = Format.TEXT;
      NamespacedId namespacedWorkspaceId = new NamespacedId(ns, workspaceId);
      String refName;
      if (workspaceId != null) {
        Map<String, String> config = ws.getWorkspace(namespacedWorkspaceId).getProperties();
        String formatStr = config.getOrDefault(PropertyIds.FORMAT, Format.TEXT.name());
        refName = config.getOrDefault(PropertyIds.NAME, Format.TEXT.name());
        format = Format.valueOf(formatStr);
      } else {
        refName = "sample";
      }

      Connection conn = getValidatedConnection(store, new NamespacedId(ns, connectionId), ConnectionType.ADLS);
      ADLSConfiguration adlsConfiguration = new ADLSConfiguration(conn.getProperties());
      Map<String, String> properties = new HashMap<>();
      properties.put("format", format.name().toLowerCase());
      String pathURI = "adl://" + adlsConfiguration.getAccountFQDN() + path;
      properties.put("path", pathURI);
      properties.put("referenceName", ReferenceNames.cleanseReferenceName(refName));
      properties.put("credentials", adlsConfiguration.getClientKey());
      properties.put("clientId", adlsConfiguration.getADLSClientID());
      properties.put("refreshTokenURL", adlsConfiguration.getEndpointURL());
      properties.put("copyHeader", String.valueOf(shouldCopyHeader(ws, namespacedWorkspaceId)));
      properties.put("schema", format.getSchema().toString());

      PluginSpec pluginSpec = new PluginSpec("AzureDataLakeStore", "source", properties);
      return new ServiceResponse<>(pluginSpec);
    }));
  }

  /**
   * get data type from the file type.
   *
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
