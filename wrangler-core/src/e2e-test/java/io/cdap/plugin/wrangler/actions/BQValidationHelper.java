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

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * BQ Validation Helper.
 */
public class BQValidationHelper {

  private static final String fileName = "/Users/bharatgulati/Desktop/WRANGLER/wrangler" +
    "/wrangler-core/src/e2e-test/resources/ExpectedBigQuery/expectedfile17";
  private static final String targetTable = "TestTable";
  private static final String targetTable2 = "TestTable9";
  private static final String targetTable10 = "WTestTable10";
  private static final String targetTable12 = "WTestTable12";
  private static final String targetTable13 = "WTestTable13";
  private static final String targetTable14 = "WTestTable14";
  private static final String targetTable15 = "WTestTable16new";
  private static final String targetTable16 = "WTestTable16_2";
  private static final String targetTable17 = "WTestTable17";
  private static final String targetTable18 = "WTestTable18";
  static List<JsonObject> bigQueryResponse = new ArrayList<>();
  static List<Object> bigQueryRows = new ArrayList<>();
  static List<JsonObject> fileData = new ArrayList<>();
  static Gson gson = new Gson();

//  public static void main(String[] args) throws IOException, InterruptedException {
//    validateActualDataToExpectedData(targetTable17, fileName);
//  }


  public static boolean validateActualDataToExpectedData(String targetTable, String fileName) throws IOException,
    InterruptedException {
    getBigQueryTableData(targetTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject json = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(json);
    }
    System.out.println(bigQueryResponse);
    getFileData(fileName);
    boolean isMatched = matchJsonLists(bigQueryResponse, fileData);
    if (isMatched) {
      System.out.println("The lists are matched.");
    } else {
      System.out.println("The lists are not matched.");
    }
    return isMatched;
  }

  public static void getFileData(String fileName) {
    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      String line;
      while ((line = br.readLine()) != null) {
        JsonObject json = gson.fromJson(line, JsonObject.class);
        fileData.add(json);
      }
    } catch (IOException e) {
      System.err.println("Error reading the file: " + e.getMessage());
    }

  }

  private static void getBigQueryTableData(String targetTable, List<Object> bigQueryRows)
    throws IOException, InterruptedException {
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + targetTable + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    result.iterateAll().forEach(value -> bigQueryRows.add(value.get(0).getValue()));

  }

  private static boolean matchJsonLists(List<JsonObject> list1, List<JsonObject> list2) {
    if (list1.size() != list2.size()) {
      return false;
    }
    for (int i = 0; i < list1.size(); i++) {
      JsonObject json1 = list1.get(i);
      JsonObject json2 = list2.get(i);
      if (!json1.equals(json2)) {
        return false;
      }
    }
    return true;
  }
}
