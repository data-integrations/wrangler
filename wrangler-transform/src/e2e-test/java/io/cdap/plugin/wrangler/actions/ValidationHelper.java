/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.wrangler.actions;

import com.esotericsoftware.minlog.Log;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.core.logging.Logger;
import io.cucumber.core.logging.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Validation Helper.
 */
public class ValidationHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationHelper.class);
  static Gson gson = new Gson();
  public static boolean validateActualDataToExpectedData(String table, String fileName) throws IOException,
    InterruptedException, URISyntaxException {
    Map<String, JsonObject> bigQueryMap = new HashMap<>();
    Map<String, JsonObject> fileMap = new HashMap<>();
    Path importExpectedFile = Paths.get(ValidationHelper.class.getResource("/" + fileName).toURI());

    getBigQueryTableData(table, bigQueryMap);
    getFileData(importExpectedFile.toString(), fileMap);

    boolean isMatched = bigQueryMap.equals(fileMap);

    return isMatched;
  }

  public static void getFileData(String fileName, Map<String, JsonObject> fileMap) {
    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      String line;
      while ((line = br.readLine()) != null) {
        JsonObject json = gson.fromJson(line, JsonObject.class);
        if (json.has("id")) { // Check if the JSON object has the "id" key
          JsonElement idElement = json.get("id");
          if (idElement.isJsonPrimitive()) {
            String idKey = idElement.getAsString();
            fileMap.put(idKey, json);
          } else {
            Log.error("ID key not found");
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Error reading the file: " + e.getMessage());
    }
  }

  private static void getBigQueryTableData(String targetTable, Map<String, JsonObject> bigQueryMap)
    throws IOException, InterruptedException {
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + targetTable + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);

    for (FieldValueList row : result.iterateAll()) {
      JsonObject json = gson.fromJson(row.get(0).getStringValue(), JsonObject.class);
      if (json.has("id")) { // Check if the JSON object has the "id" key
        JsonElement idElement = json.get("id");
        if (idElement.isJsonPrimitive()) {
          String idKey = idElement.getAsString();
          bigQueryMap.put(idKey, json);
        } else {
          LOG.error("Data Mismatched");
        }
      } else {
        LOG.error("ID Key not found in JSON object");
      }
    }
  }
}
