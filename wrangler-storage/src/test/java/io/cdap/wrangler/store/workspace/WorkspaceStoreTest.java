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

package io.cdap.wrangler.store.workspace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.dataset.workspace.WorkspaceNotFoundException;
import io.cdap.wrangler.proto.workspace.v2.SampleSpec;
import io.cdap.wrangler.proto.workspace.v2.Workspace;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceDetail;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceId;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

public class WorkspaceStoreTest extends SystemAppTestBase {
  private static WorkspaceStore store;

  @BeforeClass
  public static void setupTest() throws Exception {
    getStructuredTableAdmin().create(WorkspaceStore.WORKSPACE_TABLE_SPEC);
    store = new WorkspaceStore(getTransactionRunner());
  }

  @After
  public void cleanupTest() throws Exception {
    store.clear();
  }

  @Test
  public void testNotFoundExceptions() throws Exception {
    WorkspaceId workspace = new WorkspaceId(new NamespaceSummary("default", "", 10L), "workspace");
    try {
      store.getWorkspace(workspace);
      Assert.fail();
    } catch (WorkspaceNotFoundException e) {
      // expected
    }

    try {
      store.updateWorkspace(workspace, Workspace.builder("dummy", workspace.getWorkspaceId()).build());
      Assert.fail();
    } catch (WorkspaceNotFoundException e) {
      // expected
    }

    try {
      store.deleteWorkspace(workspace);
      Assert.fail();
    } catch (WorkspaceNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testCRUD() throws IOException {
    NamespaceSummary ns1 = new NamespaceSummary("n1", "", 10L);
    NamespaceSummary ns2 = new NamespaceSummary("n2", "", 10L);
    SampleSpec dummySpec = new SampleSpec("conn", "dummy", "/tmp", ImmutableSet.of());

    // test writes
    WorkspaceId id1 = new WorkspaceId(ns1);
    Workspace meta1 = Workspace.builder("name1", id1.getWorkspaceId())
                        .setSampleSpec(dummySpec)
                        .setCreatedTimeMillis(100L)
                        .setUpdatedTimeMillis(100L)
                        .build();
    WorkspaceDetail detail1 = new WorkspaceDetail(meta1,
                                                  Collections.singletonList(new Row(ImmutableList.of("k1", "k2"))));
    store.saveWorkspace(id1, detail1);

    WorkspaceId id2 = new WorkspaceId(ns2);
    Workspace meta2 = Workspace.builder("name2", id2.getWorkspaceId())
                        .setSampleSpec(dummySpec)
                        .setCreatedTimeMillis(200L)
                        .setUpdatedTimeMillis(400L)
                        .build();
    WorkspaceDetail detail2 = new WorkspaceDetail(meta2,
                                                  Collections.singletonList(new Row(ImmutableList.of("k3", "k4"))));
    store.saveWorkspace(id2, detail2);

    Assert.assertEquals(meta1, store.getWorkspace(id1));
    Assert.assertEquals(meta2, store.getWorkspace(id2));
    Assert.assertEquals(detail1, store.getWorkspaceDetail(id1));
    Assert.assertEquals(detail2, store.getWorkspaceDetail(id2));

    // test update
    meta1 = Workspace.builder("newname1", id1.getWorkspaceId())
              .setSampleSpec(dummySpec)
              .setCreatedTimeMillis(200L)
              .setUpdatedTimeMillis(300L)
              .setDirectives(ImmutableList.of("d1", "d2", "d3"))
              .build();
    detail1 = new WorkspaceDetail(meta1, Collections.singletonList(new Row(ImmutableList.of("k5", "k6"))));
    store.saveWorkspace(id1, detail1);

    // creation time should not change
    Workspace expected = Workspace.builder(meta1).setCreatedTimeMillis(100L).build();
    Assert.assertEquals(expected, store.getWorkspace(id1));
    Assert.assertEquals(new WorkspaceDetail(expected, detail1.getSample()), store.getWorkspaceDetail(id1));

    // test update doesn't modify sample
    meta1 = Workspace.builder("newname2", id1.getWorkspaceId())
              .setSampleSpec(dummySpec)
              .setCreatedTimeMillis(300L)
              .setUpdatedTimeMillis(400L)
              .setDirectives(ImmutableList.of("d1", "d2", "d3", "d4"))
              .build();
    store.updateWorkspace(id1, meta1);
    expected = Workspace.builder(meta1).setCreatedTimeMillis(100L).build();
    Assert.assertEquals(expected, store.getWorkspace(id1));
    Assert.assertEquals(detail1.getSample(), store.getWorkspaceDetail(id1).getSample());
    Assert.assertEquals(new WorkspaceDetail(expected, detail1.getSample()), store.getWorkspaceDetail(id1));

    // test lists don't include from other namespaces
    Assert.assertEquals(Collections.singletonList(meta1), store.listWorkspaces(ns1));
    Assert.assertEquals(Collections.singletonList(meta2), store.listWorkspaces(ns2));

    // add new one to ns1
    WorkspaceId id3 = new WorkspaceId(ns1);
    Workspace meta3 = Workspace.builder("name3", id3.getWorkspaceId())
                        .setSampleSpec(dummySpec)
                        .setCreatedTimeMillis(2000L)
                        .setUpdatedTimeMillis(4000L)
                        .build();
    WorkspaceDetail detail3 = new WorkspaceDetail(meta3,
                                                  Collections.singletonList(new Row(ImmutableList.of("k7", "k8"))));
    store.saveWorkspace(id3, detail3);
    // order can be different because of different id
    Assert.assertEquals(ImmutableSet.of(expected, meta3), new HashSet<>(store.listWorkspaces(ns1)));

    // test delete is within the correct namespace
    store.deleteWorkspace(id2);
    Assert.assertTrue(store.listWorkspaces(ns2).isEmpty());

    Assert.assertEquals(ImmutableSet.of(expected, meta3), new HashSet<>(store.listWorkspaces(ns1)));
    Assert.assertEquals(meta1, store.getWorkspace(id1));
    Assert.assertEquals(meta3, store.getWorkspace(id3));
  }

  @Test
  public void testNamespaceGenerations() {
    NamespaceSummary nsGen1 = new NamespaceSummary("ns1", "", 1L);
    NamespaceSummary nsGen2 = new NamespaceSummary("ns1", "", 2L);

    WorkspaceId id1 = new WorkspaceId(nsGen1);
    WorkspaceId id2 = new WorkspaceId(nsGen2, id1.getWorkspaceId());

    // test creation in different namespaces
    Workspace meta1 = Workspace.builder("name1", id1.getWorkspaceId())
                        .setCreatedTimeMillis(0L)
                        .setUpdatedTimeMillis(0L)
                        .build();
    store.saveWorkspace(id1, new WorkspaceDetail(meta1, Collections.emptyList()));

    // test that fetching with a different generation doesn't include the workspace
    try {
      store.getWorkspace(id2);
      Assert.fail();
    } catch (WorkspaceNotFoundException e) {
      // expected
    }

    // test that listing with a different generation doesn't include the workspace
    Assert.assertTrue(store.listWorkspaces(nsGen2).isEmpty());
  }
}
