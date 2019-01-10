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

package co.cask.wrangler.service.explorer;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.PropertyIds;
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
import co.cask.wrangler.proto.connection.ConnectionType;
import co.cask.wrangler.proto.file.FileConnectionSample;
import co.cask.wrangler.proto.file.FileSpec;
import co.cask.wrangler.sampling.Bernoulli;
import co.cask.wrangler.sampling.Poisson;
import co.cask.wrangler.sampling.Reservoir;
import co.cask.wrangler.service.common.AbstractWranglerHandler;
import co.cask.wrangler.service.common.Format;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.common.base.Charsets;
import org.apache.twill.filesystem.Location;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * A {@link FilesystemExplorer} is a HTTP Service handler for exploring the filesystem.
 * It provides capabilities for listing file(s) and directories. It also provides metadata.
 */
public class FilesystemExplorer extends AbstractWranglerHandler {
  private Explorer explorer;
  private static final String COLUMN_NAME = "body";
  private static final int FILE_SIZE = 10 * 1024 * 1024;

  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("explorer/fs")
  @GET
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @QueryParam("path") String path,
                   @QueryParam("hidden") boolean hidden) {
    list(request, responder, getContext().getNamespace(), path, hidden);
  }

  /**
   * Lists the content of the path specified using the {@link Location}.
   *
   * @param request HTTP Request Handler
   * @param responder HTTP Response Handler
   * @param path to the location in the filesystem
   */
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("contexts/{context}/explorer/fs")
  @GET
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("context") String namespace, @QueryParam("path") String path,
                   @QueryParam("hidden") boolean hidden) {
    respond(request, responder, namespace, () -> explorer.browse(path, hidden));
  }

  @Path("explorer/fs/read")
  @GET
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @QueryParam("path") String path, @QueryParam("lines") int lines,
                   @QueryParam("sampler") String sampler,
                   @QueryParam("fraction") double fraction,
                   @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    read(request, responder, getContext().getNamespace(), path, lines, sampler, fraction, scope);
  }

  /**
   * Given a path, reads the file into the workspace.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param path to the location in the filesystem.
   * @param lines number of lines to extracted from file if it's a text/plain.
   * @param sampler sampling method to be used.
   */
  @Path("contexts/{context}/explorer/fs/read")
  @GET
  public void read(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                   @QueryParam("path") String path, @QueryParam("lines") int lines,
                   @QueryParam("sampler") String sampler,
                   @QueryParam("fraction") double fraction,
                   @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, () -> {
      String header = request.getHeader(PropertyIds.CONTENT_TYPE);

      if (header == null) {
        throw new BadRequestException("Content-Type header not specified.");
      }

      FileConnectionSample sample;
      if (header.equalsIgnoreCase("text/plain") || header.contains("text/")) {
        sample = loadSampleableFile(namespace, scope, path, lines, fraction, sampler);
      } else if (header.equalsIgnoreCase("application/xml")) {
        sample = loadFile(namespace, scope, path, DataType.RECORDS);
      } else if (header.equalsIgnoreCase("application/json")) {
        sample = loadFile(namespace, scope, path, DataType.TEXT);
      } else if (header.equalsIgnoreCase("application/avro")
        || header.equalsIgnoreCase("application/protobuf")
        || header.equalsIgnoreCase("application/excel")
        || header.contains("image/")) {
        sample = loadFile(namespace, scope, path, DataType.BINARY);
      } else {
        throw new BadRequestException("Currently doesn't support wrangling of this type of file.");
      }
      return new ServiceResponse<>(sample);
    });
  }

  @Path("explorer/fs/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @QueryParam("path") String path, @QueryParam("wid") String workspaceId) {
    specification(request, responder, getContext().getNamespace(), path, workspaceId);
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param path to the location in the filesystem.
   */
  @Path("contexts/{context}/explorer/fs/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace,
                            @QueryParam("path") String path, @QueryParam("wid") String workspaceId) {
    respond(request, responder, namespace, () -> {
      Format format = Format.TEXT;
      NamespacedId namespacedId = NamespacedId.of(namespace, workspaceId);
      if (workspaceId != null) {
        Map<String, String> config = ws.getWorkspace(namespacedId).getProperties();
        String formatStr = config.getOrDefault(PropertyIds.FORMAT, Format.TEXT.name());
        format = Format.valueOf(formatStr);
      }
      Map<String, String> properties = new HashMap<>();
      properties.put("format", format.name().toLowerCase());
      Location location = explorer.getLocation(path);
      properties.put("path", location.toURI().toString());
      properties.put("referenceName", location.getName());
      properties.put("ignoreNonExistingFolders", "false");
      properties.put("recursive", "false");
      properties.put("copyHeader", String.valueOf(shouldCopyHeader(namespacedId)));
      properties.put("schema", format.getSchema().toString());

      PluginSpec pluginSpec = new PluginSpec("File", "source", properties);
      FileSpec fileSpec = new FileSpec(pluginSpec);
      return new ServiceResponse<>(fileSpec);
    });
  }

  private FileConnectionSample loadFile(String namespace, String scope, String path,
                                        DataType type) throws ExplorerException, IOException {
    Location location = explorer.getLocation(path);
    if (!location.exists()) {
      throw new BadRequestException(String.format("%s (No such file)", path));
    }

    if (location.length() > FILE_SIZE) {
      throw new BadRequestException("Files larger than 10MB are currently not supported.");
    }

    // Creates workspace.
    String name = location.getName();
    String id = String.format("%s:%s:%s:%d", scope, location.getName(),
                              location.toURI().getPath(), System.nanoTime());
    id = ServiceUtils.generateMD5(id);
    Map<String, String> properties = new HashMap<>();
    properties.put(PropertyIds.FILE_NAME, location.getName());
    properties.put(PropertyIds.URI, location.toURI().toString());
    properties.put(PropertyIds.FILE_PATH, location.toURI().getPath());
    properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.FILE.getType());
    properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
    Format format = type == DataType.BINARY ? Format.BLOB : Format.TEXT;
    properties.put(PropertyIds.FORMAT, format.name());
    NamespacedId namespacedId = NamespacedId.of(namespace, id);
    WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(namespacedId, name)
      .setScope(scope)
      .setProperties(properties)
      .build();
    ws.writeWorkspaceMeta(workspaceMeta);

    byte[] bytes = new byte[(int) location.length() + 1];
    try (BufferedInputStream stream = new BufferedInputStream(location.getInputStream())) {
      stream.read(bytes);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Write records to workspace.
    if (type == DataType.RECORDS) {
      List<Row> rows = new ArrayList<>();
      rows.add(new Row(COLUMN_NAME, new String(bytes, Charsets.UTF_8)));
      ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
      byte[] data = serDe.toByteArray(rows);
      ws.updateWorkspaceData(namespacedId, DataType.RECORDS, data);
    } else if (type == DataType.BINARY || type == DataType.TEXT) {
      ws.updateWorkspaceData(namespacedId, type, bytes);
    }

    return new FileConnectionSample(id, name, ConnectionType.FILE.getType(),
                                    SamplingMethod.NONE.getMethod(), null,
                                    location.toURI().toString(), location.toURI().getPath(),
                                    location.getName());
  }

  private FileConnectionSample loadSampleableFile(String namespace, String scope, String path, int lines,
                                                  double fraction, String sampler)
    throws IOException, ExplorerException {
    SamplingMethod samplingMethod = SamplingMethod.fromString(sampler);
    if (sampler == null || sampler.isEmpty() || SamplingMethod.fromString(sampler) == null) {
      samplingMethod = SamplingMethod.FIRST;
    }

    Location location = explorer.getLocation(path);
    if (!location.exists()) {
      throw new BadRequestException(String.format("%s (No such file)", path));
    }
    String name = location.getName();
    String id = String.format("%s:%s:%s:%d", scope, location.getName(),
                              location.toURI().getPath(), System.nanoTime());
    id = ServiceUtils.generateMD5(id);
    // Set all properties and write to workspace.
    Map<String, String> properties = new HashMap<>();
    properties.put(PropertyIds.FILE_NAME, location.getName());
    properties.put(PropertyIds.URI, location.toURI().toString());
    properties.put(PropertyIds.FILE_PATH, location.toURI().getPath());
    properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.FILE.getType());
    properties.put(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
    NamespacedId namespacedId = NamespacedId.of(namespace, id);
    WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(namespacedId, name)
      .setScope(scope)
      .setProperties(properties)
      .build();
    ws.writeWorkspaceMeta(workspaceMeta);

    // Iterate through lines to extract only 'limit' random lines.
    // Depending on the type, the sampling of the input is performed.
    List<Row> rows = new ArrayList<>();
    BoundedLineInputStream blis = BoundedLineInputStream.iterator(location.getInputStream(), Charsets.UTF_8, lines);
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
    ws.updateWorkspaceData(namespacedId, DataType.RECORDS, data);

    return new FileConnectionSample(id, name, ConnectionType.FILE.getType(),
                                    samplingMethod.getMethod(), null,
                                    location.toURI().toString(), location.toURI().getPath(),
                                    location.getName());
  }

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    final HttpServiceContext ctx = context;
    Security.addProvider(new BouncyCastleProvider());
    this.explorer = new Explorer(new DatasetProvider() {
      @Override
      public Dataset acquire() {
        return ctx.getDataset("dataprepfs");
      }

      @Override
      public void release(Dataset dataset) {
        ctx.discardDataset(dataset);
      }
    });
  }
}
