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

package io.cdap.plugin.common.stepsdesign;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Setup BQ for Wrangler tests.
 */
public class TestSetupHooks {

  public static String gcsSourceBucketName = StringUtils.EMPTY;

  @Before(order = 1, value = "@BQ_SOURCE_CSV_TEST")
  public static void createTempSourceBQTable() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQTableQueryFileCsv"),
        PluginPropertyUtils.pluginProp("InsertBQDataQueryFileCsv"));
  }

  @Before(order = 2, value = "@BQ_SINK_TEST")
  public static void setTempTargetBQTableName() {
    String bqTargetTableName = "E2E_TARGET_" + UUID.randomUUID().toString().replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("bqTargetTable", bqTargetTableName);
    BeforeActions.scenario.write("BQ Target table name - " + bqTargetTableName);
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    String bqTargetTableName = PluginPropertyUtils.pluginProp("bqTargetTable");
    try {
      BigQueryClient.dropBqQuery(bqTargetTableName);
      BeforeActions.scenario.write(
          "BQ Target table - " + bqTargetTableName + " deleted successfully");
      PluginPropertyUtils.removePluginProp("bqTargetTable");
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTableName + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * Create BigQuery table test.
   */
  @Before(order = 1, value = "@BQ_SOURCE_FXDLEN_TEST")
  public static void createTempSourceBQTableFxdLen() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQDataQueryFileFxdLen"),
        PluginPropertyUtils.pluginProp("InsertBQDataQueryFileFxdLen"));
  }

  @Before(order = 1, value = "@BQ_SOURCE_HL7_TEST")
  public static void createTempSourceBQTableHl7() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQDataQueryFileHl7"),
        PluginPropertyUtils.pluginProp("InsertBQDataQueryFileHl7"));
  }

  @Before(order = 1, value = "@BQ_SOURCE_TS_TEST")
  public static void createTempSourceBQTableTimestamp() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQDataQueryFileTimestamp"),
        PluginPropertyUtils.pluginProp("InsertBQDataQueryFileTimestamp"));
  }

  @Before(order = 1, value = "@BQ_SOURCE_DATETIME_TEST")
  public static void createTempSourceBQTableDateTime() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQDataQueryFileDatetime"),
        PluginPropertyUtils.pluginProp("InsertBQDataQueryFileDatetime"));
  }

  @After(order = 2, value = "@BQ_SOURCE_TEST")
  public static void deleteTempSourceBQTable() throws IOException, InterruptedException {
    String bqSourceTable = PluginPropertyUtils.pluginProp("bqSourceTable");
    BigQueryClient.dropBqQuery(bqSourceTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
    PluginPropertyUtils.removePluginProp("bqSourceTable");
  }
  @Before(order = 1, value = "@BQ_SOURCE_AVRO_TEST")
  public static void createTempSourceBQTableAvro() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQTableQueryFileAvro"),
            PluginPropertyUtils.pluginProp("InsertBQDataQueryFileAvro"));
  }
  @Before(order = 1, value = "@BQ_SOURCE_LOG_TEST")
  public static void createTempSourceBQTableLog() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQTableQueryFileLog"),
            PluginPropertyUtils.pluginProp("InsertBQDataQueryFileLog"));
  }


  @Before(order = 1, value = "@BQ_SOURCE_JSON_TEST")
  public static void createTempSourceBQTableJson() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQTableQueryFileJson"),
        PluginPropertyUtils.pluginProp("InsertBQDataQueryFileJson"));
  }

  @Before(order = 1, value = "@BQ_SOURCE_XML_TEST")
  public static void createTempSourceBQTableXml() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQDataQueryFileXml"),
        PluginPropertyUtils.pluginProp("InsertBQDataQueryFileXml"));
  }

  @Before(order = 1, value = "@BQ_SOURCE_GRPBY_TEST")
  public static void createTempSourceBQTableGroupBy() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQTableQueryFile"),
                                   PluginPropertyUtils.pluginProp("InsertBQDataQueryFile"));
  }

  @Before(order = 1, value = "@GCS_SOURCE_TEST")
  public static void createBucketWithEXCELFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("testFile"));
    PluginPropertyUtils.addPluginProp("gcsSourceBucket", "gs://" + gcsSourceBucketName + "/" +
        PluginPropertyUtils.pluginProp("testFile"));
    BeforeActions.scenario.write("GCS source bucket1 name - " + gcsSourceBucketName);
  }

  private static String createGCSBucketWithFile(String filePath)
      throws IOException, URISyntaxException {
    String bucketName = StorageClient.createBucket("e2e-test-" + UUID.randomUUID()).getName();
    StorageClient.uploadObject(bucketName, filePath, filePath);
    return bucketName;
  }

  @After(order = 1, value = "@GCS_SOURCE_TEST")
  public static void deleteSourceBucketWithFile() {
    deleteGCSBucket(gcsSourceBucketName);
    gcsSourceBucketName = StringUtils.EMPTY;
  }

  private static void deleteGCSBucket(String bucketName) {
    try {
      for (Blob blob : StorageClient.listObjects(bucketName).iterateAll()) {
        StorageClient.deleteObject(bucketName, blob.getName());
      }
      StorageClient.deleteBucket(bucketName);
      BeforeActions.scenario.write("Deleted GCS Bucket " + bucketName);
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        BeforeActions.scenario.write("GCS Bucket " + bucketName + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }


  private static void createSourceBQTableWithQueries(String bqCreateTableQueryFile,
      String bqInsertDataQueryFile)
      throws IOException, InterruptedException {
    String bqSourceTable =
        "E2E_SOURCE_" + UUID.randomUUID().toString().substring(0, 5).replaceAll("-",
            "_");

    String createTableQuery = StringUtils.EMPTY;
    try {
      createTableQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
        ("/" + bqCreateTableQueryFile).toURI()))
        , StandardCharsets.UTF_8);
      createTableQuery = createTableQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
        .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqCreateTableQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
                    "- error in reading create table query file " + e.getMessage());
    }

    String insertDataQuery = StringUtils.EMPTY;
    try {
      insertDataQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
          ("/" + bqInsertDataQueryFile).toURI()))
          , StandardCharsets.UTF_8);
      insertDataQuery = insertDataQuery.replace("DATASET",
              PluginPropertyUtils.pluginProp("dataset"))
          .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write(
          "Exception in reading " + bqInsertDataQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
          "- error in reading insert data query file " + e.getMessage());
    }
    BigQueryClient.getSoleQueryResult(createTableQuery);
    try {
      BigQueryClient.getSoleQueryResult(insertDataQuery);
    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@BQ_CONNECTION")
  public static void setBQConnectionName() {
    PluginPropertyUtils.addPluginProp("bqConnectionName", "BQ-" + UUID.randomUUID());
  }
}
