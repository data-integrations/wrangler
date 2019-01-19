/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.wrangler.dataset.connections.ConnectionAlreadyExistsException;
import co.cask.wrangler.dataset.connections.ConnectionNotFoundException;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionMeta;
import co.cask.wrangler.proto.connection.ConnectionType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

/**
 * Tests for the {@link ConnectionStore}.
 */
public class ConnectionStoreTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void testNotFoundException() throws Exception {
    addDatasetInstance("table", "notfoundtest");
    DataSetManager<Table> tableManager = getDataset("notfoundtest");
    Table table = tableManager.get();
    ConnectionStore connectionStore = new ConnectionStore(table);

    NamespacedId id = new NamespacedId("c0", "id");
    try {
      connectionStore.get(id);
      Assert.fail("Getting a non-existent connection did not throw an exception.");
    } catch (ConnectionNotFoundException e) {
      // expected
    }

    try {
      connectionStore.update(id, ConnectionMeta.builder().setName("name").setType(ConnectionType.FILE).build());
      Assert.fail("Getting a non-existent connection did not throw an exception.");
    } catch (ConnectionNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testAlreadyExistsException() throws Exception {
    addDatasetInstance("table", "alreadyExistsTest");
    DataSetManager<Table> tableManager = getDataset("alreadyExistsTest");
    Table table = tableManager.get();
    ConnectionStore connectionStore = new ConnectionStore(table);

    ConnectionMeta meta = ConnectionMeta.builder().setName("name").setType(ConnectionType.FILE).build();
    connectionStore.create("c0", meta);
    try {
      connectionStore.create("c0", meta);
      Assert.fail("Creating a duplicate connection did not throw an exception.");
    } catch (ConnectionAlreadyExistsException e) {
      // expected
    }
  }

  @Test
  public void testCRUD() throws Exception {
    addDatasetInstance("table", "crudtest");
    DataSetManager<Table> tableManager = getDataset("crudtest");
    Table table = tableManager.get();
    ConnectionStore connectionStore = new ConnectionStore(table);

    String ns = "n0";
    Assert.assertTrue(connectionStore.list(ns, x -> true).isEmpty());

    // test creation
    ConnectionMeta expected = ConnectionMeta.builder()
      .setName("My Connection")
      .setType(ConnectionType.FILE)
      .putProperty("k1", "v1")
      .build();
    NamespacedId id = connectionStore.create(ns, expected);
    tableManager.flush();

    Connection actual = connectionStore.get(id);
    assertConnectionMetaEquality(expected, actual);

    // test update
    ConnectionMeta updated = ConnectionMeta.builder()
      .setName(expected.getName())
      .setType(expected.getType())
      .putProperty("k1", "v2")
      .build();
    connectionStore.update(id, updated);
    tableManager.flush();
    actual = connectionStore.get(id);
    assertConnectionMetaEquality(updated, actual);

    // test scan with non-matching filter
    Assert.assertTrue(connectionStore.list(ns, c -> c.getType() == ConnectionType.DATABASE).isEmpty());
    // text scan
    List<Connection> list = connectionStore.list(ns, x -> true);
    Assert.assertEquals(1, list.size());
    assertConnectionMetaEquality(updated, actual);

    // test delete
    connectionStore.delete(id);
    tableManager.flush();
    try {
      connectionStore.get(id);
      Assert.fail("Connection was not properly deleted.");
    } catch (ConnectionNotFoundException e) {
      // expected
    }
    Assert.assertTrue(connectionStore.list(ns, x -> true).isEmpty());
  }

  @Test
  public void testNamespaceIsolation() throws Exception {
    addDatasetInstance("table", "nstest");
    DataSetManager<Table> tableManager = getDataset("nstest");
    Table table = tableManager.get();
    ConnectionStore connectionStore = new ConnectionStore(table);

    String ns1 = "ns1";
    String ns2 = "ns2";

    // test creation in different namespaces
    ConnectionMeta expected1 = ConnectionMeta.builder()
      .setName("My Connection")
      .setType(ConnectionType.FILE)
      .putProperty("k1", "v1")
      .build();
    NamespacedId id1 = connectionStore.create(ns1, expected1);
    ConnectionMeta expected2 = ConnectionMeta.builder()
      .setName("My Connection")
      .setType(ConnectionType.GCS)
      .putProperty("k2", "v2")
      .build();
    NamespacedId id2 = connectionStore.create(ns2, expected2);
    tableManager.flush();

    Connection actual1 = connectionStore.get(id1);
    assertConnectionMetaEquality(expected1, actual1);
    Connection actual2 = connectionStore.get(id2);
    assertConnectionMetaEquality(expected2, actual2);

    List<Connection> list1 = connectionStore.list(ns1, x -> true);
    Assert.assertEquals(1, list1.size());
    assertConnectionMetaEquality(expected1, list1.iterator().next());
    List<Connection> list2 = connectionStore.list(ns2, x -> true);
    Assert.assertEquals(1, list2.size());
    assertConnectionMetaEquality(expected2, list2.iterator().next());

    // test deletion from one namespace
    connectionStore.delete(id1);
    tableManager.flush();
    Assert.assertTrue(connectionStore.list(ns1, x -> true).isEmpty());
    try {
      connectionStore.get(id1);
      Assert.fail("Connection store did not properly delete connection 1");
    } catch (ConnectionNotFoundException e) {
      // expected
    }
    actual2 = connectionStore.get(id2);
    assertConnectionMetaEquality(expected2, actual2);
    list2 = connectionStore.list(ns2, x -> true);
    Assert.assertEquals(1, list2.size());
    assertConnectionMetaEquality(expected2, list2.iterator().next());
  }

  private void assertConnectionMetaEquality(ConnectionMeta expected, Connection actual) {
    // can't just compare objects since the store sets created, id, and updated fields
    Assert.assertEquals(expected.getName(), actual.getName());
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getProperties(), actual.getProperties());
  }
}
