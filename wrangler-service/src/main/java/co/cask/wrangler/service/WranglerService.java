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
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.internal.TextDirectives;
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
  @Path("workspaces/{workspace}")
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
   * Deletes the workspace.
   *
   * @param request
   * @param responder
   * @param ws
   */
  @DELETE
  @Path("workspaces/{workspace}")
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
  @Path("workspaces/{workspace}/upload")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("workspace") String ws ) {

    String body = null;
    ByteBuffer content = request.getContent();
    if (content != null && content.hasRemaining()) {
      body = Bytes.toString(content);
    }

    if (body == null || body.isEmpty()) {
      error(responder, "Body not present, please post the file containing the records to be wrangle.");
      return;
    }

    List<Record> records = new ArrayList<>();
    int i = 0;
    for (String line : body.split("\n")) {
      records.add(new Record(ws, line));
      ++i;
    }

    String d = new Gson().toJson(records);
    try {
      Put data = new Put (Bytes.toBytes(ws), Bytes.toBytes("data"), Bytes.toBytes(d));
      workspace.put(data);
      success(responder, String.format("Successfully uploaded data to workspace '%s' (records %d)", ws, i));
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  @GET
  @Path("workspaces/{workspace}/download")
  public void download(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("workspace") String ws ) {

    try {
      Row row = workspace.get(Bytes.toBytes(ws));

      String data = row.getString("data");
      if (data == null || data.isEmpty()) {
        error(responder, "No data exists in the workspace. Please upload the data to this workspace.");
        return;
      }

      List<Record> records = new Gson().fromJson(data, new TypeToken<List<Record>>(){}.getType());
      JSONArray values = new JSONArray();
      for (Record record : records) {
        List<KeyValue<String, Object>> fields = record.getFields();
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
      response.put("value", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (DataSetException e) {
      LOG.info("Failed", e);
      error(responder, e.getMessage());
    }
  }

  @GET
  @Path("workspaces/{workspace}/schema")
  public void schema(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("workspace") String ws,
                        @QueryParam("directive") List<String> directives) {
    try {
      Row rawRows = workspace.get(Bytes.toBytes(ws));
      List<Record> records = new Gson().fromJson(rawRows.getString("data"),
                                                 new TypeToken<List<Record>>(){}.getType());

      records = records.subList(0, Math.min(2, records.size()));
      List<Record> newRecords = execute(records, directives.toArray(new String[directives.size()]));
      Record r = newRecords.get(0);

      List<KeyValue<String, Object>> columns = r.getFields();
      JSONArray values = new JSONArray();
      for (KeyValue<String, Object> column : columns) {
        JSONObject col = new JSONObject();
        col.put("name", column.getKey());
        Object v = column.getValue();
        JSONArray t = new JSONArray();
        String type = "string";
        if (v instanceof Integer) {
          type = "int";
        } else if (v instanceof Long) {
          type = "long";
        } else if (v instanceof Double) {
          type = "double";
        } else if (v instanceof Float) {
          type = "float";
        }
        t.put(type);
        t.put("null");
        col.put("type", t);
        values.put(col);
      }

      sendJson(responder, HttpURLConnection.HTTP_OK, values.toString());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }

  }

  @GET
  @Path("workspaces/{workspace}/execute")
  public void directive(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("workspace") String ws,
                        @QueryParam("directive") List<String> directives,
                        @QueryParam("limit") int limit) {

    try {
      Row rawRows = workspace.get(Bytes.toBytes(ws));
      List<Record> records = new Gson().fromJson(rawRows.getString("data"),
                                                 new TypeToken<List<Record>>(){}.getType());

      List<Record> newRecords = execute(records.subList(0, limit), directives.toArray(new String[directives.size()]));
      JSONArray values = new JSONArray();
      JSONArray headers = new JSONArray();
      boolean added = false;
      for (Record record : newRecords) {
        List<KeyValue<String, Object>> fields = record.getFields();
        JSONObject r = new JSONObject();
        for (KeyValue<String, Object> field : fields) {
          if (added == false) {
            headers.put(field.getKey());
          }
          r.put(field.getKey(), field.getValue().toString());
        }
        added = true;
        values.put(r);
      }

      JSONObject response = new JSONObject();
      response.put("status", HttpURLConnection.HTTP_OK);
      response.put("message", "Success");
      response.put("items", newRecords.size());
      response.put("header", headers);
      response.put("value", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }


  // Application Platform System - Big Data Appliance
  private List<Record>  execute (List<Record> records, String[] directives)
    throws DirectiveParseException, StepException {
    Directives specification = new TextDirectives(directives);
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
