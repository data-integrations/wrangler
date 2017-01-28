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

package co.cask.wrangler.service;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.SkipRecordException;
import co.cask.wrangler.api.Specification;
import co.cask.wrangler.api.SpecificationParseException;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.internal.TextSpecification;
import com.google.gson.Gson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Created by nitin on 1/27/17.
 */
public class WranglerService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(WranglerService.class);
  public static final String WORKSPACE_DATASET = "workspace";

  @UseDataSet(WORKSPACE_DATASET)
  private Table workspace;

  /**
   *
   * @param request
   * @param responder
   * @param ws
   */
  @PUT
  @Path("v1/workspaces/{workspace}")
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("workspace") String ws) {
    try {
      Put created
        = new Put (Bytes.toBytes(ws), Bytes.toBytes("created"), Bytes.toBytes((long)System.currentTimeMillis()/1000));
      workspace.put(created);
      success(responder, String.format("Successfully created workspace '%s'", ws));
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   *
   * @param request
   * @param responder
   * @param ws
   */
  @DELETE
  @Path("v1/workspaces/{workspace}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("workspace") String ws) {
    try {
      workspace.delete(Bytes.toBytes(ws));
      success(responder, String.format("Successfully deleted workspace '%s'", ws));
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  @POST
  @Path("v1/workspaces/{workspace}/upload")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("workspace") String ws ) {

    String contentType = request.getHeader("Content");
    
    String body = null;
    ByteBuffer content = request.getContent();
    if (content != null && content.hasRemaining()) {
      body = Bytes.toString(content);
    }

    if (body == null || body.isEmpty()) {
      error(responder, "Body not present, please post the event JSON to generate paths.");
      return;
    }

    List<Record> records = new ArrayList<>();
    String[] lines = body.split("\n");
    for (String line : lines) {
      records.add(new Record(ws, line));
    }
    String d = new Gson().toJson(records);
    try {
      Put data = new Put (Bytes.toBytes(ws), Bytes.toBytes("data"), Bytes.toBytes(d));
      workspace.put(data);
      success(responder, String.format("Successfully uploaded data to workspace '%s'", ws));
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  @GET
  @Path("v1/workspaces/{workspace}/download")
  public void download(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("workspace") String ws ) {

    try {
      Row row = workspace.get(Bytes.toBytes(ws));
      List<Record> records = new Gson().fromJson(row.getString("data"),
                                                 new TypeToken<List<Record>>(){}.getType());
      JSONArray values = new JSONArray();
      for (Record record : records) {
        LOG.info(record.toString());
        List<KeyValue<String, Object>> fields = record.getRecord();
        JSONObject r = new JSONObject();
        for (KeyValue<String, Object> field : fields) {
          r.append(field.getKey(), field.getValue().toString());
        }
        values.put(r);
      }
      JSONObject response = new JSONObject();
      response.put("status", HttpURLConnection.HTTP_OK);
      response.put("message", "Success");
      response.put("items", records.size());
      response.put("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  @GET
  @Path("v1/workspaces/{workspace}/execute")
  public void directive(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("workspace") String ws,
                        @QueryParam("directive") List<String> directives,
                        @QueryParam("limit") int limit) {

    try {
      Row rawRows = workspace.get(Bytes.toBytes(ws));
      List<Record> records = new Gson().fromJson(rawRows.getString("data"),
                                                 new TypeToken<List<Record>>(){}.getType());

      List<Record> newRecords = execute(records, directives.toArray(new String[directives.size()]), limit);
      JSONArray values = new JSONArray();
      for (Record record : newRecords) {
        List<KeyValue<String, Object>> fields = record.getRecord();
        JSONObject r = new JSONObject();
        for (KeyValue<String, Object> field : fields) {
          r.put(field.getKey(), field.getValue().toString());
        }
        values.put(r);
      }

      JSONObject response = new JSONObject();
      response.put("status", HttpURLConnection.HTTP_OK);
      response.put("message", "Success");
      response.put("items", newRecords.size());
      response.put("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }


  // Application Platform System - Big Data Appliance
  private List<Record>  execute (List<Record> records, String[] directives, int limit)
    throws SpecificationParseException, StepException, SkipRecordException {
    Specification specification = new TextSpecification(directives);
    List<Step> steps = specification.getSteps();

    for (Step step : steps) {
      records = step.execute(records, null);
    }

    return records;
  }


  /**
   * Sends the error response back to client.
   *
   * @param responder to respond to the service request.
   * @param message to be included as part of the error
   */
  static final void error(HttpServiceResponder responder, String message) {
    JSONObject error = new JSONObject();
    error.put("status", HttpURLConnection.HTTP_INTERNAL_ERROR);
    error.put("message", message);
    sendJson(responder, HttpURLConnection.HTTP_INTERNAL_ERROR, error.toString(2));
  }

  /**
   * Returns a Json response back to client.
   *
   * @param responder to respond to the service request.
   * @param status code to be returned to client.
   * @param body to be sent back to client.
   */
  static final void sendJson(HttpServiceResponder responder, int status, String body) {
    responder.send(status, ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)),
                   "application/json", new HashMap<String, String>());
  }

  /**
   * Sends the success response back to client.
   *
   * @param responder to respond to the service request.
   * @param message to be included as part of the error
   */
  static final void success(HttpServiceResponder responder, String message) {
    JSONObject error = new JSONObject();
    error.put("status", HttpURLConnection.HTTP_OK);
    error.put("message", message);
    sendJson(responder, HttpURLConnection.HTTP_OK, error.toString(2));
  }

}
