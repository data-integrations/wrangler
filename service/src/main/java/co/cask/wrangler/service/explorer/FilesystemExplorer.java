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

package co.cask.wrangler.service.explorer;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.sampling.Bernoulli;
import co.cask.wrangler.sampling.Poisson;
import co.cask.wrangler.sampling.Reservoir;
import co.cask.wrangler.ServiceUtils;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.twill.filesystem.Location;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;
import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

/**
 * A {@link FilesystemExplorer} is a HTTP Service handler for exploring the filesystem.
 * It provides capabilities for listing file(s) and directories. It also provides metadata.
 */
public class FilesystemExplorer extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(FilesystemExplorer.class);
  private static final Gson gson =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private Explorer explorer;
  private static final String COLUMN_NAME = "body";
  private static final int FILE_SIZE = 10 * 1024 * 1024;

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset table;

  /**
   * Lists the content of the path specified using the {@Location}.
   *
   * @param request HTTP Request Handler
   * @param responder HTTP Response Handler
   * @param path to the location in the filesystem
   * @throws Exception
   */
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("explorer/fs")
  @GET
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @QueryParam("path") String path, @QueryParam("hidden") boolean hidden) throws Exception {
    try {
      Map<String, Object> listing = explorer.browse(path, hidden);
      sendJson(responder, HttpURLConnection.HTTP_OK, gson.toJson(listing));
    } catch (ExplorerException e) {
      error(responder, e.getMessage());
    }
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
  @Path("explorer/fs/read")
  @GET
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @QueryParam("path") String path, @QueryParam("lines") int lines,
                   @QueryParam("sampler") String sampler, @QueryParam("fraction") double fraction) {
    RequestExtractor extractor = new RequestExtractor(request);
    if (extractor.isContentType("text/plain") || extractor.isContentType("application/json")) {
      loadSamplableFile(responder, path, lines, fraction, sampler);
    } else if (extractor.isContentType("application/xml")) {
      loadFile(responder, path, DataType.RECORDS);
    } else if (extractor.isContentType("application/avro") || extractor.isContentType("application/protobuf")) {
      loadFile(responder, path, DataType.BINARY);
    } else {
      error(responder, "Currently doesn't support wrangling of this type of file.");
    }
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param path to the location in the filesystem.
   */
  @Path("explorer/fs/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @QueryParam("path") String path) {
    JsonObject response = new JsonObject();
    try {
      Location location = explorer.getLocation(path);
      JsonObject value = new JsonObject();
      JsonObject file = new JsonObject();
      Map<String, String> properties = new HashMap<>();
      properties.put("path", location.toURI().toString());
      properties.put("referenceName", location.getName());
      properties.put("ignoreNonExistingFolders", "false");
      properties.put("recursive", "false");
      file.add("properties", gson.toJsonTree(properties));
      file.addProperty("name", "File");
      file.addProperty("type", "source");
      value.add("File", file);
      JsonArray values = new JsonArray();
      values.add(value);
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (ExplorerException e) {
      error(responder, e.getMessage());
    }
  }

  private void loadFile(HttpServiceResponder responder, String path, DataType type) {
    JsonObject response = new JsonObject();
    BufferedInputStream stream = null;
    try {
      Location location = explorer.getLocation(path);
      if (!location.exists()) {
        error(responder, String.format("%s (No such file)", path));
        return;
      }

      if (location.length() > FILE_SIZE) {
        error(responder, "Large files greater than 10MG not supported.");
        return;
      }

      // Creates workspace.
      String name = location.getName();
      String id = String.format("%s:%s", location.getName(), location.toURI().getPath());
      id = ServiceUtils.generateMD5(id);
      table.createWorkspaceMeta(id, name);

      stream = new BufferedInputStream(location.getInputStream());
      byte[] bytes = new byte[(int)location.length() + 1];
      stream.read(bytes);

      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.FILE_NAME, location.getName());
      properties.put(PropertyIds.URI, location.toURI().toString());
      properties.put(PropertyIds.FILE_PATH, location.toURI().getPath());
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.FILE.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      table.writeProperties(id, properties);

      // Write records to workspace.
      if(type == DataType.RECORDS) {
        List<Record> records = new ArrayList<>();
        records.add(new Record(COLUMN_NAME, new String(bytes, Charsets.UTF_8)));
        String data = gson.toJson(records);
        table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.RECORDS, data.getBytes(Charsets.UTF_8));
      } else if (type == DataType.BINARY) {
        table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.BINARY, bytes);
      }

      // Preparing return response to include mandatory fields : id and name.
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty(PropertyIds.ID, id);
      object.addProperty(PropertyIds.NAME, name);
      object.addProperty(PropertyIds.URI, location.toURI().toString());
      object.addProperty(PropertyIds.FILE_PATH, location.toURI().getPath());
      object.addProperty(PropertyIds.FILE_NAME, location.getName());
      object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      values.add(object);

      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e){

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

  private void loadSamplableFile(HttpServiceResponder responder,
                                 String path, int lines, double fraction, String sampler) {
    JsonObject response = new JsonObject();
    SamplingMethod samplingMethod = SamplingMethod.fromString(sampler);
    if (sampler == null || sampler.isEmpty() || SamplingMethod.fromString(sampler) == null) {
      samplingMethod = SamplingMethod.FIRST;
    }
    BoundedLineInputStream stream = null;
    try {
      Location location = explorer.getLocation(path);
      if (!location.exists()) {
        error(responder, String.format("%s (No such file)", path));
        return;
      }
      String name = location.getName();
      String id = String.format("%s:%s", location.getName(), location.toURI().getPath());
      id = ServiceUtils.generateMD5(id);
      table.createWorkspaceMeta(id, name);

      // Iterate through lines to extract only 'limit' random lines.
      // Depending on the type, the sampling of the input is performed.
      List<Record> records = new ArrayList<>();
      BoundedLineInputStream blis = BoundedLineInputStream.iterator(location.getInputStream(), Charsets.UTF_8, lines);
      Iterator<String> it = blis;
      if (samplingMethod == SamplingMethod.POISSON) {
        it = new Poisson<String>(fraction).sample(blis);
      } else if (samplingMethod == SamplingMethod.BERNOULLI) {
        it = new Bernoulli<String>(fraction).sample(blis);
      } else if (samplingMethod == SamplingMethod.RESERVOIR) {
        it = new Reservoir<String>(lines).sample(blis);
      }
      while(it.hasNext()) {
        records.add(new Record(COLUMN_NAME, it.next()));
      }

      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.FILE_NAME, location.getName());
      properties.put(PropertyIds.URI, location.toURI().toString());
      properties.put(PropertyIds.FILE_PATH, location.toURI().getPath());
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.FILE.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
      table.writeProperties(id, properties);

      // Write records to workspace.
      String data = gson.toJson(records);
      table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.RECORDS, data.getBytes(Charsets.UTF_8));

      // Preparing return response to include mandatory fields : id and name.
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty(PropertyIds.ID, id);
      object.addProperty(PropertyIds.NAME, name);
      object.addProperty(PropertyIds.URI, location.toURI().toString());
      object.addProperty(PropertyIds.FILE_PATH, location.toURI().getPath());
      object.addProperty(PropertyIds.FILE_NAME, location.getName());
      object.addProperty(PropertyIds.SAMPLER_TYPE, samplingMethod.getMethod());
      values.add(object);

      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (ExplorerException e) {
      error(responder, e.getMessage());
    } catch (IOException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
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
