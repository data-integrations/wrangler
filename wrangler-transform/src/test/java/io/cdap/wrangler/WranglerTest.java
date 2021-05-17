/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test for wrangler
 */
@Ignore
public class WranglerTest extends HydratorTestBase {
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static final Gson GSON = new GsonBuilder()
                                     .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  private static int startCount = 0;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  @BeforeClass
  public static void setUpTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("wrangler-transform", "1.0.0"), APP_ARTIFACT_ID, Wrangler.class);
  }

  @Test
  public void testWranglerDefaultFailure() throws Exception {
    testWranglerDefaultFailure(Engine.MAPREDUCE);
    testWranglerDefaultFailure(Engine.SPARK);
  }

  private void testWranglerDefaultFailure(Engine engine) throws Exception {
    String srcTableName = "src" + engine;
    String sinkTableName = "sink" + engine;

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    ETLStage wranglerStage =
      new ETLStage("wrangler", new ETLPlugin("Wrangler", Transform.PLUGIN_TYPE,
                                             ImmutableMap.of("schema", GSON.toJson(schema),
                                                             "field", "*",
                                                             // this directive to make sure the pipeline fail
                                                             "directives", "set-type :name integer",
                                                             "precondition", "false")));

    // source -> wrangler -> sink
    ETLBatchConfig config = ETLBatchConfig.builder()
                              .setEngine(engine)
                              .addStage(new ETLStage("source", MockSource.getPlugin(srcTableName)))
                              .addStage(wranglerStage)
                              .addStage(new ETLStage("sink", MockSink.getPlugin(sinkTableName)))
                              .addConnection("source", "wrangler")
                              .addConnection("wrangler", "sink")
                              .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("testApp" + engine);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord samuel = StructuredRecord.builder(schema).set("id", 1).set("name", "samuel").build();
    StructuredRecord dwayne = StructuredRecord.builder(schema).set("id", 2).set("name", "dwayne").build();

    DataSetManager<Table> sourceTable = getDataset(srcTableName);
    MockSource.writeInput(sourceTable, ImmutableList.of(samuel, dwayne));

    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);

    // start the pipeline and ensure pipeline failed
    manager.startAndWaitForRun(ProgramRunStatus.FAILED, 3, TimeUnit.MINUTES);
  }
}
