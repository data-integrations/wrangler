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

package io.cdap.wrangler.dataset;

import com.google.gson.JsonObject;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.wrangler.dataset.workspace.DataType;
import io.cdap.wrangler.dataset.workspace.Workspace;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.dataset.workspace.WorkspaceMeta;
import io.cdap.wrangler.dataset.workspace.WorkspaceNotFoundException;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.Recipe;
import io.cdap.wrangler.proto.Request;
import io.cdap.wrangler.proto.RequestV1;
import io.cdap.wrangler.proto.Sampling;
import io.cdap.wrangler.proto.WorkspaceIdentifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Tests for the workspace dataset.
 */
public class WorkspaceDatasetTest extends SystemAppTestBase {

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(WorkspaceDataset.TABLE_SPEC);
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(WorkspaceDataset.TABLE_SPEC.getTableId());
  }
  
  @Test
  public void testNotFoundExceptions() throws Exception {
    getTransactionRunner().run(context -> {
      WorkspaceDataset ws = WorkspaceDataset.get(context);
      Namespace namespace = new Namespace("c0", 10L);
      try {
        ws.updateWorkspaceData(new NamespacedId(namespace, "id"), DataType.TEXT, new byte[]{0});
        Assert.fail("Updating a non-existing workspace should fail.");
      } catch (WorkspaceNotFoundException e) {
        // expected
      }

      try {
        ws.updateWorkspaceProperties(new NamespacedId(namespace, "id"), Collections.emptyMap());
        Assert.fail("Updating a non-existing workspace should fail.");
      } catch (WorkspaceNotFoundException e) {
        // expected
      }

      try {
        ws.updateWorkspaceRequest(new NamespacedId(namespace, "id"), null);
        Assert.fail("Updating a non-existing workspace should fail.");
      } catch (WorkspaceNotFoundException e) {
        // expected
      }
    });
  }

  @Test
  public void testScopes() {
    WorkspaceIdentifier id1 = new WorkspaceIdentifier("id1", "name1");
    WorkspaceIdentifier id2 = new WorkspaceIdentifier("id2", "name2");

    Namespace namespace = new Namespace("c0", 10L);

    String scope1 = "scope1";
    String scope2 = "scope2";
    WorkspaceMeta meta1 = WorkspaceMeta.builder(id1.getName())
      .setScope(scope1)
      .build();
    WorkspaceMeta meta2 = WorkspaceMeta.builder(id2.getName())
      .setScope(scope2)
      .build();

    run(ws -> ws.writeWorkspaceMeta(new NamespacedId(namespace, id1.getId()), meta1));
    run(ws -> ws.writeWorkspaceMeta(new NamespacedId(namespace, id2.getId()), meta2));

    Assert.assertEquals(Collections.singletonList(id1), call(ws -> ws.listWorkspaces(namespace, scope1)));
    Assert.assertEquals(Collections.singletonList(id2), call(ws -> ws.listWorkspaces(namespace, scope2)));

    run(ws -> ws.deleteScope(namespace, scope1));

    Assert.assertTrue(call(ws -> ws.listWorkspaces(namespace, scope1).isEmpty()));
    Assert.assertEquals(Collections.singletonList(id2), call(ws -> ws.listWorkspaces(namespace, scope2)));

    run(ws -> ws.deleteWorkspace(new NamespacedId(namespace, id2.getId())));
    Assert.assertTrue(call(ws -> ws.listWorkspaces(namespace, scope1).isEmpty()));
    Assert.assertTrue(call(ws -> ws.listWorkspaces(namespace, scope2).isEmpty()));
  }

  @Test
  public void testNamespaceIsolation() {
    Namespace ns1 = new Namespace("n1", 10L);
    Namespace ns2 = new Namespace("n2", 10L);

    // test writes in different namespaces don't conflict with each other
    WorkspaceMeta meta1 = WorkspaceMeta.builder("name1")
      .setType(DataType.BINARY)
      .setProperties(Collections.singletonMap("k1", "v1"))
      .build();
    NamespacedId id1 = call(ws -> ws.createWorkspace(ns1, meta1));
    WorkspaceMeta meta2 = WorkspaceMeta.builder("name2")
      .setType(DataType.BINARY)
      .setProperties(Collections.singletonMap("k2", "v2"))
      .build();
    NamespacedId id2 = call(ws -> ws.createWorkspace(ns2, meta2));

    Workspace actual1 = call(ws -> ws.getWorkspace(id1));
    Workspace expected1 = Workspace.builder(id1, meta1.getName())
      .setCreated(actual1.getCreated())
      .setUpdated(actual1.getUpdated())
      .setScope(meta1.getScope())
      .setType(meta1.getType())
      .setProperties(meta1.getProperties())
      .build();
    Assert.assertEquals(expected1, actual1);

    Workspace actual2 = call(ws -> ws.getWorkspace(id2));
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
                        call(ws -> ws.listWorkspaces(id1.getNamespace(), WorkspaceDataset.DEFAULT_SCOPE)));
    Assert.assertEquals(Collections.singletonList(new WorkspaceIdentifier(id2.getId(), meta2.getName())),
                        call(ws -> ws.listWorkspaces(id2.getNamespace(), WorkspaceDataset.DEFAULT_SCOPE)));

    // test delete is within the correct namespace
    run(ws -> ws.deleteWorkspace(id2));

    Assert.assertEquals(Collections.singletonList(new WorkspaceIdentifier(id1.getId(), meta1.getName())),
                        call(ws -> ws.listWorkspaces(id1.getNamespace(), WorkspaceDataset.DEFAULT_SCOPE)));
    Assert.assertTrue(call(ws -> ws.listWorkspaces(id2.getNamespace(), WorkspaceDataset.DEFAULT_SCOPE).isEmpty()));
    Assert.assertEquals(expected1, call(ws -> ws.getWorkspace(id1)));
  }

  @Test
  public void testNamespaceGenerations() {
    Namespace nsGen1 = new Namespace("ns1", 1L);
    Namespace nsGen2 = new Namespace("ns1", 2L);

    // test creation in different namespaces
    WorkspaceMeta meta1 = WorkspaceMeta.builder("name1")
      .setType(DataType.BINARY)
      .setProperties(Collections.singletonMap("k1", "v1"))
      .build();
    NamespacedId id1 = call(ws -> ws.createWorkspace(nsGen1, meta1));

    // test that fetching with a different generation doesn't include the connection
    try {
      run(ws -> ws.getWorkspace(new NamespacedId(nsGen2, id1.getId())));
      Assert.fail("workspace with a different generation should not be visible.");
    } catch (WorkspaceNotFoundException e) {
      // expected
    }

    // test that listing with a different generation doesn't include the connection
    Assert.assertTrue(call(ws -> ws.listWorkspaces(nsGen2, WorkspaceDataset.DEFAULT_SCOPE)).isEmpty());
  }

  @Test
  public void testCRUD() {
    Namespace ns = new Namespace("c0", 10L);
    Assert.assertTrue(call(ws -> ws.listWorkspaces(ns, "default").isEmpty()));

    // test write and get
    WorkspaceMeta meta1 = WorkspaceMeta.builder("name")
      .setScope("default")
      .setType(DataType.BINARY)
      .setProperties(Collections.singletonMap("k1", "v1"))
      .build();
    NamespacedId id = call(ws -> ws.createWorkspace(ns, meta1));
    WorkspaceIdentifier workspaceId = new WorkspaceIdentifier(id.getId(), meta1.getName());

    Workspace actual = call(ws -> ws.getWorkspace(id));
    Workspace expected = Workspace.builder(id, meta1.getName())
      .setCreated(actual.getCreated())
      .setUpdated(actual.getUpdated())
      .setType(meta1.getType())
      .setScope(meta1.getScope())
      .setProperties(meta1.getProperties())
      .build();
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(Collections.singletonList(workspaceId), call(ws -> ws.listWorkspaces(ns, "default")));

    // test updating properties
    Map<String, String> properties = Collections.singletonMap("k2", "v2");
    run(ws -> ws.updateWorkspaceProperties(id, properties));
    actual = call(ws -> ws.getWorkspace(id));
    expected = Workspace.builder(expected)
      .setUpdated(actual.getUpdated())
      .setProperties(properties)
      .build();
    Assert.assertEquals(expected, actual);

    // test updating request
    io.cdap.wrangler.proto.Workspace requestWorkspace = new io.cdap.wrangler.proto.Workspace(meta1.getName(), 10);
    Recipe recipe = new Recipe(Collections.singletonList("parse-as-csv body"), true, "recipeName");
    Sampling sampling = new Sampling("random", 0, 10);
    Request request = new RequestV1(requestWorkspace, recipe, sampling, new JsonObject());
    run(ws -> ws.updateWorkspaceRequest(id, request));
    actual = call(ws -> ws.getWorkspace(id));
    expected = Workspace.builder(expected)
      .setUpdated(actual.getUpdated())
      .setRequest(request)
      .build();
    Assert.assertEquals(expected, actual);

    // test updating data
    byte[] data = new byte[]{0, 1, 2};
    run(ws -> ws.updateWorkspaceData(id, DataType.RECORDS, data));
    actual = call(ws -> ws.getWorkspace(id));
    expected = Workspace.builder(expected)
      .setType(DataType.RECORDS)
      .setUpdated(actual.getUpdated())
      .setData(data)
      .build();
    Assert.assertEquals(expected, actual);

    // test updating meta
    WorkspaceMeta meta2 = WorkspaceMeta.builder(meta1.getName())
      .setType(DataType.TEXT)
      .setProperties(Collections.singletonMap("k3", "v3"))
      .build();
    run(ws -> ws.writeWorkspaceMeta(id, meta2));
    actual = call(ws -> ws.getWorkspace(id));
    expected = Workspace.builder(expected)
      .setUpdated(actual.getUpdated())
      .setProperties(meta2.getProperties())
      .setType(meta2.getType())
      .build();
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(Collections.singletonList(workspaceId), call(ws -> ws.listWorkspaces(ns, "default")));

    // delete workspace
    run(ws -> ws.deleteWorkspace(id));
    Assert.assertTrue(call(ws -> ws.listWorkspaces(ns, "default").isEmpty()));
    try {
      call(ws -> ws.getWorkspace(id));
      Assert.fail("Workspace was not deleted.");
    } catch (WorkspaceNotFoundException e) {
      // expected
    }
  }
  
  private <T> T call(WorkspaceCallable<T> callable) {
    return TransactionRunners.run(getTransactionRunner(), context -> {
      WorkspaceDataset ws = WorkspaceDataset.get(context);
      return callable.run(ws);
    }, WorkspaceNotFoundException.class);
  }

  private void run(WorkspaceRunnable runnable) {
    TransactionRunners.run(getTransactionRunner(), context -> {
      WorkspaceDataset ws = WorkspaceDataset.get(context);
      runnable.run(ws);
    }, WorkspaceNotFoundException.class);
  }

  private interface WorkspaceRunnable {
    void run(WorkspaceDataset ws) throws Exception;
  }

  private interface WorkspaceCallable<T> {
    T run(WorkspaceDataset ws) throws Exception;
  }
}
