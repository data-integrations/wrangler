/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.wrangler.dataset;

import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.Workspace;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceMeta;
import co.cask.wrangler.dataset.workspace.WorkspaceNotFoundException;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.Recipe;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.proto.RequestV1;
import co.cask.wrangler.proto.Sampling;
import co.cask.wrangler.proto.WorkspaceIdentifier;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Tests for the workspace dataset.
 */
public class WorkspaceDatasetTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void testNotFoundExceptions() throws Exception {
    addDatasetInstance("table", "notfoundTest");
    DataSetManager<Table> tableManager = getDataset("notfoundTest");
    Table table = tableManager.get();
    WorkspaceDataset workspaceDataset = new WorkspaceDataset(table);

    try {
      workspaceDataset.updateWorkspaceData(NamespacedId.of("c0", "id"), DataType.TEXT, new byte[] { 0 });
      Assert.fail("Updating a non-existing workspace should fail.");
    } catch (WorkspaceNotFoundException e) {
      // expected
    }

    try {
      workspaceDataset.updateWorkspaceProperties(NamespacedId.of("c0", "id"), Collections.emptyMap());
      Assert.fail("Updating a non-existing workspace should fail.");
    } catch (WorkspaceNotFoundException e) {
      // expected
    }

    try {
      workspaceDataset.updateWorkspaceRequest(NamespacedId.of("c0", "id"), null);
      Assert.fail("Updating a non-existing workspace should fail.");
    } catch (WorkspaceNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testScopes() throws Exception {
    addDatasetInstance("table", "scopeTest");
    DataSetManager<Table> tableManager = getDataset("scopeTest");
    Table table = tableManager.get();
    WorkspaceDataset workspaceDataset = new WorkspaceDataset(table);

    WorkspaceIdentifier id1 = new WorkspaceIdentifier("id1", "name1");
    WorkspaceIdentifier id2 = new WorkspaceIdentifier("id2", "name2");

    String context = "c0";

    String scope1 = "scope1";
    String scope2 = "scope2";
    WorkspaceMeta meta1 = WorkspaceMeta.builder(NamespacedId.of(context, id1.getId()), id1.getName())
      .setScope(scope1)
      .build();
    WorkspaceMeta meta2 = WorkspaceMeta.builder(NamespacedId.of(context, id2.getId()), id2.getName())
      .setScope(scope2)
      .build();

    workspaceDataset.writeWorkspaceMeta(meta1);
    workspaceDataset.writeWorkspaceMeta(meta2);
    tableManager.flush();

    Assert.assertEquals(Collections.singletonList(id1), workspaceDataset.listWorkspaces(context, scope1));
    Assert.assertEquals(Collections.singletonList(id2), workspaceDataset.listWorkspaces(context, scope2));

    workspaceDataset.deleteScope(context, scope1);
    tableManager.flush();

    Assert.assertTrue(workspaceDataset.listWorkspaces(context, scope1).isEmpty());
    Assert.assertEquals(Collections.singletonList(id2), workspaceDataset.listWorkspaces(context, scope2));

    workspaceDataset.deleteWorkspace(NamespacedId.of(context, id2.getId()));
    tableManager.flush();
    Assert.assertTrue(workspaceDataset.listWorkspaces(context, scope1).isEmpty());
    Assert.assertTrue(workspaceDataset.listWorkspaces(context, scope2).isEmpty());
  }

  @Test
  public void testNamespaceIsolation() throws Exception {
    addDatasetInstance("table", "nsTest");
    DataSetManager<Table> tableManager = getDataset("nsTest");
    Table table = tableManager.get();
    WorkspaceDataset workspaceDataset = new WorkspaceDataset(table);

    NamespacedId id1 = NamespacedId.of("n1", "id1");
    NamespacedId id2 = NamespacedId.of("n2", "id2");

    // test writes in different namespaces don't conflict with each other
    WorkspaceMeta meta1 = WorkspaceMeta.builder(id1, "name1")
      .setType(DataType.BINARY)
      .setProperties(Collections.singletonMap("k1", "v1"))
      .build();
    workspaceDataset.writeWorkspaceMeta(meta1);
    WorkspaceMeta meta2 = WorkspaceMeta.builder(id2, "name2")
      .setType(DataType.BINARY)
      .setProperties(Collections.singletonMap("k2", "v2"))
      .build();
    workspaceDataset.writeWorkspaceMeta(meta2);
    tableManager.flush();

    Workspace actual1 = workspaceDataset.getWorkspace(id1);
    Workspace expected1 = Workspace.builder(id1, meta1.getName())
      .setCreated(actual1.getCreated())
      .setUpdated(actual1.getUpdated())
      .setScope(meta1.getScope())
      .setType(meta1.getType())
      .setProperties(meta1.getProperties())
      .build();
    Assert.assertEquals(expected1, actual1);

    Workspace actual2 = workspaceDataset.getWorkspace(id2);
    Workspace expected2 = Workspace.builder(id2, meta2.getName())
      .setCreated(actual2.getCreated())
      .setUpdated(actual2.getUpdated())
      .setScope(meta2.getScope())
      .setType(meta2.getType())
      .setProperties(meta2.getProperties())
      .build();
    Assert.assertEquals(expected2, actual2);

    // test lists don't include from other namespaces
    Assert.assertEquals(Collections.singletonList(new WorkspaceIdentifier(id1.getId(), meta1.getName())),
                        workspaceDataset.listWorkspaces(id1.getNamespace(), WorkspaceDataset.DEFAULT_SCOPE));
    Assert.assertEquals(Collections.singletonList(new WorkspaceIdentifier(id2.getId(), meta2.getName())),
                        workspaceDataset.listWorkspaces(id2.getNamespace(), WorkspaceDataset.DEFAULT_SCOPE));

    // test delete is within the correct namespace
    workspaceDataset.deleteWorkspace(id2);
    tableManager.flush();

    Assert.assertEquals(Collections.singletonList(new WorkspaceIdentifier(id1.getId(), meta1.getName())),
                        workspaceDataset.listWorkspaces(id1.getNamespace(), WorkspaceDataset.DEFAULT_SCOPE));
    Assert.assertTrue(workspaceDataset.listWorkspaces(id2.getNamespace(), WorkspaceDataset.DEFAULT_SCOPE).isEmpty());
    Assert.assertEquals(expected1, workspaceDataset.getWorkspace(id1));
  }

  @Test
  public void testCRUD() throws Exception {
    addDatasetInstance("table", "crudtest");
    DataSetManager<Table> tableManager = getDataset("crudtest");
    Table table = tableManager.get();
    WorkspaceDataset workspaceDataset = new WorkspaceDataset(table);

    NamespacedId id = NamespacedId.of("c0", "id0");
    Assert.assertTrue(workspaceDataset.listWorkspaces(id.getNamespace(), "default").isEmpty());
    Assert.assertFalse(workspaceDataset.hasWorkspace(id));
    WorkspaceIdentifier workspaceId = new WorkspaceIdentifier(id.getId(), "name");

    // test write and get
    WorkspaceMeta meta = WorkspaceMeta.builder(id, workspaceId.getName())
      .setScope("default")
      .setType(DataType.BINARY)
      .setProperties(Collections.singletonMap("k1", "v1"))
      .build();
    workspaceDataset.writeWorkspaceMeta(meta);
    tableManager.flush();

    Workspace actual = workspaceDataset.getWorkspace(id);
    Workspace expected = Workspace.builder(id, meta.getName())
      .setCreated(actual.getCreated())
      .setUpdated(actual.getUpdated())
      .setType(meta.getType())
      .setScope(meta.getScope())
      .setProperties(meta.getProperties())
      .build();
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(Collections.singletonList(workspaceId),
                        workspaceDataset.listWorkspaces(id.getNamespace(), "default"));

    // test updating properties
    Map<String, String> properties = Collections.singletonMap("k2", "v2");
    workspaceDataset.updateWorkspaceProperties(id, properties);
    tableManager.flush();
    actual = workspaceDataset.getWorkspace(id);
    expected = Workspace.builder(expected)
      .setUpdated(actual.getUpdated())
      .setProperties(properties)
      .build();
    Assert.assertEquals(expected, actual);

    // test updating request
    co.cask.wrangler.proto.Workspace requestWorkspace = new co.cask.wrangler.proto.Workspace(meta.getName(), 10);
    Recipe recipe = new Recipe(Collections.singletonList("parse-as-csv body"), true, "recipeName");
    Sampling sampling = new Sampling("random", 0, 10);
    Request request = new RequestV1(requestWorkspace, recipe, sampling, new JsonObject());
    workspaceDataset.updateWorkspaceRequest(id, request);
    tableManager.flush();
    actual = workspaceDataset.getWorkspace(id);
    expected = Workspace.builder(expected)
      .setUpdated(actual.getUpdated())
      .setRequest(request)
      .build();
    Assert.assertEquals(expected, actual);

    // test updating data
    byte[] data = new byte[] { 0, 1, 2 };
    workspaceDataset.updateWorkspaceData(id, DataType.RECORDS, data);
    tableManager.flush();
    actual = workspaceDataset.getWorkspace(id);
    expected = Workspace.builder(expected)
      .setType(DataType.RECORDS)
      .setUpdated(actual.getUpdated())
      .setData(data)
      .build();
    Assert.assertEquals(expected, actual);

    // test updating meta
    meta = WorkspaceMeta.builder(id, meta.getName())
      .setType(DataType.TEXT)
      .setProperties(Collections.singletonMap("k3", "v3"))
      .build();
    workspaceDataset.writeWorkspaceMeta(meta);
    tableManager.flush();
    actual = workspaceDataset.getWorkspace(id);
    expected = Workspace.builder(expected)
      .setUpdated(actual.getUpdated())
      .setProperties(meta.getProperties())
      .setType(meta.getType())
      .build();
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(Collections.singletonList(workspaceId),
                        workspaceDataset.listWorkspaces(id.getNamespace(), "default"));

    // delete workspace
    workspaceDataset.deleteWorkspace(id);
    tableManager.flush();
    Assert.assertTrue(workspaceDataset.listWorkspaces(id.getNamespace(), "default").isEmpty());
    try {
      workspaceDataset.getWorkspace(id);
      Assert.fail("Workspace was not deleted.");
    } catch (WorkspaceNotFoundException e) {
      // expected
    }
  }

}
