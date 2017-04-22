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

package co.cask.wrangler.service.directive;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.statistics.Statistics;
import co.cask.wrangler.api.validator.Validator;
import co.cask.wrangler.api.validator.ValidatorException;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.executor.TextDirectives;
import co.cask.wrangler.executor.UsageRegistry;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.statistics.BasicStatistics;
import co.cask.wrangler.validator.ColumnNameValidator;
import co.cask.wrangler.sampling.Reservoir;
import co.cask.wrangler.utils.Json2Schema;
import co.cask.wrangler.utils.RecordConvertorException;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Service for managing workspaces and also application of directives on to the workspace.
 */
public class DirectivesService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DirectivesService.class);
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  public static final String WORKSPACE_DATASET = "workspace";
  private static final String resourceName = ".properties";

  @UseDataSet(WORKSPACE_DATASET)
  private Table workspace;

  /**
   * Creates a workspace.
   *
   * Following is the response
   *
   * {
   *   "status" : 200,
   *   "message" : Successfully created workspace 'test'.
   * }
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param ws Workspace to be created.
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
   * Lists all workspaces.
   *
   * Following is a response returned
   *
   * {
   *   "status" : 200,
   *   "message" : "Success",
   *   "count" : 4,
   *   "values" : [
   *      "body",
   *      "ws",
   *      "test",
   *      "message"
   *   ]
   * }
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @GET
  @Path("workspaces")
  public void list(HttpServiceRequest request, HttpServiceResponder responder) {
    JsonObject response = new JsonObject();
    Row row;
    try {
      try (Scanner scanner = workspace.scan(null, null)) {
        JsonArray values = new JsonArray();
        while((row = scanner.next()) != null) {
          byte[] key = row.getRow();
          values.add(new JsonPrimitive(new String(key)));
        }
        response.addProperty("status", HttpURLConnection.HTTP_OK);
        response.addProperty("message", "Success");
        response.addProperty("count", values.size());
        response.add("values", values);
        sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
      }
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Deletes the workspace.
   *
   * Following is the response
   *
   * {
   *   "status" : 200,
   *   "message" : Successfully deleted workspace 'test'.
   * }
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param ws Workspace to deleted.
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

  /**
   * Get information about the workspace.
   *
   * Following is the response
   *
   * {
   *   "status" : 200,
   *   "message" : "Success",
   *   "values" : [
   *     {
   *       "workspace" : "data",
   *       "created" : 1430202202,
   *       "recipe" : [
   *          "parse-as-csv data ,",
   *          "drop data"
   *       ]
   *     }
   *   ]
   * }
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param ws Workspace to deleted.
   */
  @GET
  @Path("workspaces/{workspace}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("workspace") String ws) {

    JsonObject response = new JsonObject();
    JsonArray values = new JsonArray();
    try {
      Row row = workspace.get(Bytes.toBytes(ws));
      byte[] bytes = row.get("request");
      if (bytes != null) {
        values.add(new JsonParser().parse(new String(bytes, StandardCharsets.UTF_8)));
        response.addProperty("count", 1);
      } else {
        response.addProperty("count", 0);
      }
      response.add("values", values);
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (DataSetException | JSONException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Upload data to the workspace.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param ws Upload data to the workspace.
   */
  @POST
  @Path("workspaces/{workspace}/upload")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("workspace") String ws) {

    String delimiter = request.getHeader("recorddelimiter");
    String charset = request.getHeader("charset");

    if (charset == null || charset.isEmpty()) {
      charset = "utf-8";
    }

    String body = null;
    ByteBuffer content = request.getContent();
    if (content != null && content.hasRemaining()) {
      body = Charset.forName(charset).decode(content).toString();
    }

    if (body == null || body.isEmpty()) {
      error(responder, "Body not present, please post the file containing the records to be wrangle.");
      return;
    }

    List<Record> records = new ArrayList<>();
    int i = 0;
    if(delimiter == null || delimiter.isEmpty()) {
      delimiter = "\\u001A";
    }

    delimiter = StringEscapeUtils.unescapeJava(delimiter);
    for (String line : body.split(delimiter)) {
      records.add(new Record(ws, line));
      ++i;
    }

    String d = GSON.toJson(records);
    try {
      Put data = new Put (Bytes.toBytes(ws), Bytes.toBytes("data"), Bytes.toBytes(d));
      workspace.put(data);
      success(responder, String.format("Successfully uploaded data to workspace '%s' (records %d)", ws, i));
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Download data from the workspace.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param ws Download data from the workspace.
   */
  @GET
  @Path("workspaces/{workspace}/download")
  public void download(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("workspace") String ws) {

    try {
      Row row = workspace.get(Bytes.toBytes(ws));
      String data = Bytes.toString(row.get("data"));
      if (data == null || data.isEmpty()) {
        error(responder, "No data exists in the workspace. Please upload the data to this workspace.");
        return;
      }
      JsonArray array = GSON.fromJson(data, new TypeToken<JsonArray>() {}.getType());
      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", array.size());
      response.add("values", array);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Summarizes the workspace by running directives.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param ws Workspace data to be summarized.
   */
  @POST
  @Path("workspaces/{workspace}/summary")
  public void summary(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("workspace") String ws) {
    try {

      // Read the request body
      Request reqBody = postRequest(request);
      if (reqBody == null) {
        error(responder, "Request is empty. Please check if the request is sent as HTTP POST body.");
        return;
      }

      List<String> directives = reqBody.getRecipe().getDirectives();
      List<Record> records = getWorkspaceData(ws, responder);
      if (records == null) {
        return;
      }

      // Get the minimum of records and the limit as set by the user.
      int limit = Math.min(reqBody.getSampling().getLimit(), records.size());

      // Randomly select a 'count' records from the input.
      Iterator<Record> sampledRecords
        = new Reservoir<Record>(limit).sample(records.iterator());

      // Run it through the pipeline.
      records =
        execute(Lists.newArrayList(sampledRecords), directives.toArray(new String[directives.size()]));

      // Final response object.
      JsonObject response = new JsonObject();

      // Storing the results 'validation' and 'statistics'
      JsonObject result = new JsonObject();

      // Validate Column names.
      Validator<String> validator = new ColumnNameValidator();
      validator.initialize();

      // Iterate through columns to get a set
      Set<String> uniqueColumns = new HashSet<>();
      for (Record record : records) {
        for (int i = 0; i < record.length(); ++i) {
          uniqueColumns.add(record.getColumn(i));
        }
      }

      JsonObject columnValidationResult = new JsonObject();
      for (String name : uniqueColumns) {
        JsonObject columnResult = new JsonObject();
        try {
          validator.validate(name);
          columnResult.addProperty("valid", true);
        } catch (ValidatorException e) {
          columnResult.addProperty("valid", false);
          columnResult.addProperty("message", e.getMessage());
        }
        columnValidationResult.add(name, columnResult);
      }

      result.add("validation", columnValidationResult);

      // Generate General and Type related Statistics for each column.
      Statistics statsGenerator = new BasicStatistics();
      Record summary = statsGenerator.aggregate(records);

      Record stats = (Record) summary.getValue("stats");
      Record types = (Record) summary.getValue("types");

      // Serialize the results into JSON.
      List<KeyValue<String, Object>> fields = stats.getFields();
      JsonObject statistics = new JsonObject();
      for (KeyValue<String, Object> field : fields) {
        List<KeyValue<String, Double>> values = (List<KeyValue<String, Double>>) field.getValue();
        JsonObject v = new JsonObject();
        JsonObject o = new JsonObject();
        for (KeyValue<String, Double> value : values) {
          o.addProperty(value.getKey(), value.getValue().floatValue()*100);
        }
        v.add("general", o);
        statistics.add(field.getKey(), v);
      }

      fields = types.getFields();
      for (KeyValue<String, Object> field : fields) {
        List<KeyValue<String, Double>> values = (List<KeyValue<String, Double>>) field.getValue();
        JsonObject v = new JsonObject();
        JsonObject o = new JsonObject();
        for (KeyValue<String, Double> value : values) {
          o.addProperty(value.getKey(), value.getValue().floatValue()*100);
        }
        v.add("types", o);
        JsonObject object = (JsonObject) statistics.get(field.getKey());
        if (object == null) {
          statistics.add(field.getKey(), v);
        } else {
          object.add("types", o);
        }
      }

      // Put the statistics along with validation rules.
      result.add("statistics", statistics);

      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", 2);
      response.add("values", result);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  @POST
  @Path("workspaces/{workspace}/schema")
  public void schema(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("workspace") String ws) {
    try {

      // Read the request body
      Request reqBody = postRequest(request);
      if (reqBody == null) {
        error(responder, "Request is empty. Please check if the request is sent as HTTP POST body.");
        return;
      }

      List<String> directives = reqBody.getRecipe().getDirectives();

      List<Record> records = getWorkspaceData(ws, responder);
      if (records == null) {
        return;
      }
      // Get the minimum of records and the limit as set by the user.
      int limit = Math.min(reqBody.getSampling().getLimit(), records.size());
      records = records.subList(0, limit);
      List<Record> newRecords = execute(records, directives.toArray(new String[directives.size()]));

      // generate a schema based upon the first record
      Json2Schema json2Schema = new Json2Schema();
      try {
        Schema schema = json2Schema.toSchema("record", createUberRecord(newRecords));
        if (schema.getType() != Schema.Type.RECORD) {
          schema = Schema.recordOf("array", Schema.Field.of("value", schema));
        }

        String schemaJson = GSON.toJson(schema);
        // the current contract with the UI is not to pass the
        // entire schema string, but just the fields
        String fieldsJson = new JsonParser().parse(schemaJson)
                                    .getAsJsonObject()
                                    .get("fields").toString();
        sendJson(responder, HttpURLConnection.HTTP_OK, fieldsJson);
      } catch (RecordConvertorException e) {
        error(responder, "There was a problem in generating schema for the record. " + e.getMessage());
        return;
      }
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Generates the capability matrix, with versions and build number.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @GET
  @Path("info")
  public void capabilities(HttpServiceRequest request, HttpServiceResponder responder) {
    JsonArray values = new JsonArray();

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties props = new Properties();
    try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
      props.load(resourceStream);
    } catch (IOException e) {
      error(responder, "There was problem reading the capability matrix. " +
        "Please check the environment to ensure you have right verions of jar." + e.getMessage());
      return;
    }

    JsonObject object = new JsonObject();
    for(String key : props.stringPropertyNames()) {
      String value = props.getProperty(key);
      object.addProperty(key, value);
    }
    values.add(object);

    JsonObject response = new JsonObject();
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "Success");
    response.addProperty("count", values.size());
    response.add("values", values);
    sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
  }

  /**
   * Executes the directives on the record stored in the workspace.
   *
   * Following is the response from this request
   * {
   *   "status" : 200,
   *   "message" : "Success",
   *   "count" : 2,
   *   "header" : [ "a", "b", "c", "d" ],
   *   "value" : [
   *     { record 1},
   *     { record 2}
   *   ]
   * }
   *
   * @param request to gather information of the request.
   * @param responder to respond to the service request.
   * @param ws workspace in which the directives are executed.
   */
  @POST
  @Path("workspaces/{workspace}/execute")
  public void directive(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("workspace") String ws) {
    try {

      // Read the request body
      Request reqBody = postRequest(request);
      if (reqBody == null) {
        error(responder, "Request is empty. Please check if the request is sent as HTTP POST body.");
        return;
      }

      List<Record> records = getWorkspaceData(ws, responder);
      if (records == null) {
        error(responder, "No data is present in the workspace '" + ws + "'");
        return;
      }

      // Get the minimum of records and the limit as set by the user.
      int limit = Math.min(records.size(), reqBody.getSampling().getLimit());

      // Extract directives from the request.
      List<String> directives = reqBody.getRecipe().getDirectives();

      // Execute the directives.
      List<Record> newRecords = execute(records.subList(0, limit),
                                        directives.toArray(new String[directives.size()]));

      JsonArray values = new JsonArray();
      JsonArray headers = new JsonArray();
      Set<String> headerList = new HashSet<>();
      boolean onlyFirstRow = true;
      int count = 0;
      for (Record record : newRecords) {
        // Limit the number of results to be returned.
        if (count >= reqBody.getWorkspace().getResults()) {
          break;
        }
        List<KeyValue<String, Object>> fields = record.getFields();
        JsonObject r = new JsonObject();
        for (KeyValue<String, Object> field : fields) {
          if (!headerList.contains(field.getKey())) {
            headers.add(new JsonPrimitive(field.getKey()));
            headerList.add(field.getKey());
          }
          Object object = field.getValue();
          if (object != null) {
            if ((object.getClass().getMethod("toString").getDeclaringClass() != Object.class)) {
              r.addProperty(field.getKey(), object.toString());
            } else {
              if (onlyFirstRow) {
                r.addProperty(field.getKey(), "Not viewable object, delete this column after extracting information.");
                onlyFirstRow = false;
              } else {
                r.addProperty(field.getKey(), ".");
              }
            }
          } else {
            r.add(field.getKey(), JsonNull.INSTANCE);
          }
        }
        values.add(r);
        count++;
      }

      // Autosaves the recipes being executed.
      autosave(ws, reqBody);

      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", count);
      response.add("header", headers);
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (JsonParseException e) {
      error(responder, "Issue parsing request. " + e.getMessage());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * @returns the Records in the specified workspace. Returns null if the workspace does not exist, in which case
   *          the HttpServiceResponder is responded to before returning.
   */
  @Nullable
  private List<Record> getWorkspace(String workspaceName, HttpServiceResponder responder) {
    Row row = workspace.get(Bytes.toBytes(workspaceName));

    String data = Bytes.toString(row.get("data"));
    if (data == null || data.isEmpty()) {
      error(responder, "No data exists in the workspace. Please upload the data to this workspace.");
      return null;
    }

    return GSON.fromJson(data, new TypeToken<List<Record>>(){}.getType());
  }

  /**
   * This REST API returns an array of all the directives, their usage and description.
   *
   * Following is the response of this call.
   * {
   *   "status": "OK",
   *   "message": "Success",
   *   "count" : 10,
   *   "values" : [
   *      {
   *        "directive" : "parse-as-csv",
   *        "usage" : "parse-as-csv <column> <delimiter> <skip-empty-row>",
   *        "description" : "Parses as CSV ..."
   *      },
   *      ...
   *   ]
   * }
   * @param request to gather information of the request.
   * @param responder to respond to the service request.
   */
  @GET
  @Path("usage")
  public void usage(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      UsageRegistry registry = new UsageRegistry();
      List<UsageRegistry.UsageDatum> usages = registry.getAll();

      JsonObject response = new JsonObject();
      int count = 0;
      JsonArray values = new JsonArray();
      for (UsageRegistry.UsageDatum entry : usages) {
        JsonObject usage = new JsonObject();
        usage.addProperty("directive", entry.getDirective());
        usage.addProperty("usage", entry.getUsage());
        usage.addProperty("description", entry.getDescription());
        values.add(usage);
        count++;
      }
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", count);
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Extracts the charsets supported.
   *
   * @param request to gather information of the request.
   * @param responder to respond to the service request.
   */
  @GET
  @Path("charsets")
  public void charsets(HttpServiceRequest request, HttpServiceResponder responder) {
    Set<String> charsets = Charset.availableCharsets().keySet();
    JsonObject response = new JsonObject();
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "success");
    response.addProperty("count", charsets.size());
    JsonArray array = new JsonArray();
    for (String charset : charsets) {
      array.add(new JsonPrimitive(charset));
    }
    response.add("values", array);
    sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
  }

  /**
   * Executes directives.
   *
   * @param records to on which directives need to be executed on.
   * @param directives to be executed.
   * @return List of processed record.
   * @throws DirectiveParseException
   * @throws StepException
   */
  private List<Record> execute(List<Record> records, String[] directives)
    throws DirectiveParseException, StepException, PipelineException {
    PipelineExecutor executor = new PipelineExecutor();
    executor.configure(new TextDirectives(directives),
                       new ServicePipelineContext(PipelineContext.Environment.SERVICE, getContext()));
    return executor.execute(records);
  }


  /**
   * Sends the error response back to client.
   *
   * @param responder to respond to the service request.
   * @param message to be included as part of the error
   */
  public static final void error(HttpServiceResponder responder, String message) {
    JsonObject error = new JsonObject();
    error.addProperty("status", HttpURLConnection.HTTP_INTERNAL_ERROR);
    error.addProperty("message", message);
    sendJson(responder, HttpURLConnection.HTTP_INTERNAL_ERROR, error.toString());
  }

  /**
   * Sends the error response back to client for not Found.
   *
   * @param responder to respond to the service request.
   * @param message to be included as part of the error
   */
  public static final void notFound(HttpServiceResponder responder, String message) {
    JsonObject error = new JsonObject();
    error.addProperty("status", HttpURLConnection.HTTP_NOT_FOUND);
    error.addProperty("message", message);
    sendJson(responder, HttpURLConnection.HTTP_NOT_FOUND, error.toString());
  }

  /**
   * Returns a Json response back to client.
   *
   * @param responder to respond to the service request.
   * @param status code to be returned to client.
   * @param body to be sent back to client.
   */
  public static final void sendJson(HttpServiceResponder responder, int status, String body) {
    responder.send(status, ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)),
                   "application/json", new HashMap<String, String>());
  }

  /**
   * Sends the success response back to client.
   *
   * @param responder to respond to the service request.
   * @param message to be included as part of the error
   */
  public static final void success(HttpServiceResponder responder, String message) {
    JsonObject error = new JsonObject();
    error.addProperty("status", HttpURLConnection.HTTP_OK);
    error.addProperty("message", message);
    sendJson(responder, HttpURLConnection.HTTP_OK, error.toString());
  }

  /**
   * @returns the Records in the specified workspace. Returns null if the workspace does not exist, in which case
   *          the HttpServiceResponder is responded to before returning.
   */
  @Nullable
  private List<Record> getWorkspaceData(String workspaceName, HttpServiceResponder responder) {
    Row row = workspace.get(Bytes.toBytes(workspaceName));

    String data = Bytes.toString(row.get("data"));
    if (data == null || data.isEmpty()) {
      error(responder, "No data exists in the workspace. Please upload the data to this workspace.");
      return null;
    }

    return GSON.fromJson(data, new TypeToken<List<Record>>(){}.getType());
  }

  /**
   * Autosaves the directives into workspace.
   *
   * @param ws Workspace under which the directives need to be saved.
   * @param request Entire request.
   * @throws DataSetException throw if it fails to save.
   */
  private void autosave(String ws, Request request) throws DataSetException {
    String recipe = GSON.toJson(request);
    Put putRecipe
      = new Put (Bytes.toBytes(ws), Bytes.toBytes("request"), Bytes.toBytes(recipe));
    workspace.put(putRecipe);
  }

  /**
   * Parses the POST request from the body.
   *
   * @param request HTTP Request to extract the body.
   * @return {@link Request}
   * @throws JsonParseException thrown if there are any issues in parsing.
   */
  private Request postRequest(HttpServiceRequest request) throws JsonParseException {
    Request body = null;
    ByteBuffer content = request.getContent();
    if (content != null && content.hasRemaining()) {
      GsonBuilder builder = new GsonBuilder();
      builder.registerTypeAdapter(Request.class, new RequestDeserializer());
      Gson gson = builder.create();
      body = gson.fromJson(Bytes.toString(content), Request.class);
      return body;
    }
    return null;
  }

  /**
   * Creates a uber record after iterating through all records.
   *
   * @param records list of all records.
   * @return A single record will rows merged across all columns.
   */
  private static Record createUberRecord(List<Record> records) {
    Record uber = new Record();
    for (Record record : records) {
      for (int i = 0; i < record.length(); ++i) {
        Object o = record.getValue(i);
        if (o != null) {
          int idx = uber.find(record.getColumn(i));
          if (idx == -1) {
            uber.add(record.getColumn(i), o);
          }
        }
      }
    }
    return uber;
  }

}
