/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.wrangler.dataset.connections.ConnectionAlreadyExistsException;
import io.cdap.wrangler.dataset.connections.ConnectionNotFoundException;
import io.cdap.wrangler.dataset.connections.ConnectionStore;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Tests for the {@link ConnectionStore}.
 */
public class ConnectionStoreTest extends SystemAppTestBase {

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(ConnectionStore.TABLE_SPEC);
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(ConnectionStore.TABLE_SPEC.getTableId());
  }

  @Test
  public void testNotFoundException() throws Exception {
    getTransactionRunner().run(context -> {
      ConnectionStore connectionStore = ConnectionStore.get(context);

      NamespacedId id = new NamespacedId(new Namespace("c0", 10L), "id");
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
    });
  }

  @Test
  public void testAlreadyExistsException() throws Exception {
    getTransactionRunner().run(context -> {
      ConnectionStore connectionStore = ConnectionStore.get(context);

      Namespace namespace = new Namespace("c0", 10L);
      ConnectionMeta meta = ConnectionMeta.builder().setName("name").setType(ConnectionType.FILE).build();
      connectionStore.create(namespace, meta);
      try {
        connectionStore.create(namespace, meta);
        Assert.fail("Creating a duplicate connection did not throw an exception.");
      } catch (ConnectionAlreadyExistsException e) {
        // expected
      }
    });
  }

  @Test
  public void testCRUD() {
    Namespace ns = new Namespace("n0", 10L);
    Assert.assertTrue(run(connectionStore -> connectionStore.list(ns, x -> true)).isEmpty());

    // test creation
    ConnectionMeta expected = ConnectionMeta.builder()
      .setName("My Connection")
      .setType(ConnectionType.FILE)
      .putProperty("k1", "v1")
      .build();
    NamespacedId id = run(connectionStore -> connectionStore.create(ns, expected));

    Connection actual = run(connectionStore -> connectionStore.get(id));
    assertConnectionMetaEquality(expected, actual);

    // test update
    ConnectionMeta updated = ConnectionMeta.builder()
      .setName(expected.getName())
      .setType(expected.getType())
      .putProperty("k1", "v2")
      .build();
    actual = run(connectionStore -> {
      connectionStore.update(id, updated);
      return connectionStore.get(id);
    });
    assertConnectionMetaEquality(updated, actual);

    // test scan with non-matching filter
    List<Connection> filterList = run(connectionStore ->
                                        connectionStore.list(ns, c -> c.getType() == ConnectionType.DATABASE));
    Assert.assertTrue(filterList.isEmpty());
    // text scan
    List<Connection> list = run(connectionStore -> connectionStore.list(ns, x -> true));
    Assert.assertEquals(1, list.size());
    assertConnectionMetaEquality(updated, actual);

    // test delete
    run(connectionStore -> {
      connectionStore.delete(id);
      try {
        connectionStore.get(id);
        Assert.fail("Connection was not properly deleted.");
      } catch (ConnectionNotFoundException e) {
        // expected
      }
      return null;
    });
    Assert.assertTrue(run(connectionStore -> connectionStore.list(ns, x -> true)).isEmpty());
  }

  @Test
  public void testNamespaceIsolation() {
    Namespace ns1 = new Namespace("ns1", 10L);
    Namespace ns2 = new Namespace("ns2", 10L);

    // test creation in different namespaces
    ConnectionMeta expected1 = ConnectionMeta.builder()
      .setName("My Connection")
      .setType(ConnectionType.FILE)
      .putProperty("k1", "v1")
      .build();
    NamespacedId id1 = run(connectionStore -> connectionStore.create(ns1, expected1));
    ConnectionMeta expected2 = ConnectionMeta.builder()
      .setName("My Connection")
      .setType(ConnectionType.GCS)
      .putProperty("k2", "v2")
      .build();
    NamespacedId id2 = run(connectionStore -> connectionStore.create(ns2, expected2));

    Connection actual1 = run(connectionStore -> connectionStore.get(id1));
    assertConnectionMetaEquality(expected1, actual1);
    Connection actual2 = run(connectionStore -> connectionStore.get(id2));
    assertConnectionMetaEquality(expected2, actual2);

    List<Connection> list1 = run(connectionStore -> connectionStore.list(ns1, x -> true));
    Assert.assertEquals(1, list1.size());
    assertConnectionMetaEquality(expected1, list1.iterator().next());
    List<Connection> list2 = run(connectionStore -> connectionStore.list(ns2, x -> true));
    Assert.assertEquals(1, list2.size());
    assertConnectionMetaEquality(expected2, list2.iterator().next());

    // test deletion from one namespace
    run(connectionStore -> {
      connectionStore.delete(id1);
      return null;
    });
    Assert.assertTrue(run(connectionStore -> connectionStore.list(ns1, x -> true)).isEmpty());
    try {
      run(connectionStore -> connectionStore.get(id1));
      Assert.fail("Connection store did not properly delete connection 1");
    } catch (ConnectionNotFoundException e) {
      // expected
    }
    actual2 = run(connectionStore -> connectionStore.get(id2));
    assertConnectionMetaEquality(expected2, actual2);
    list2 = run(connectionStore -> connectionStore.list(ns2, x -> true));
    Assert.assertEquals(1, list2.size());
    assertConnectionMetaEquality(expected2, list2.iterator().next());
  }

  @Test
  public void testNamespaceGenerations() {
    Namespace nsGen1 = new Namespace("ns1", 1L);
    Namespace nsGen2 = new Namespace("ns1", 2L);


    // test creation in different namespaces
    ConnectionMeta expected1 = ConnectionMeta.builder()
      .setName("My Connection")
      .setType(ConnectionType.FILE)
      .putProperty("k1", "v1")
      .build();
    NamespacedId id1 = run(connectionStore -> connectionStore.create(nsGen1, expected1));

    // test that fetching with a different generation doesn't include the connection
    try {
      run(connectionStore -> connectionStore.get(new NamespacedId(nsGen2, id1.getId())));
      Assert.fail("connection with a different generation should not be visible.");
    } catch (ConnectionNotFoundException e) {
      // expected
    }

    // test that listing with a different generation doesn't include the connection
    Assert.assertTrue(run(connectionStore -> connectionStore.list(nsGen2, x -> true)).isEmpty());
  }

  @Test
  public void testPreconfiguredConnections() {
    Namespace nsGen1 = new Namespace("ns1", 1L);

    ConnectionMeta preconfiguredMeta = ConnectionMeta.builder()
      .setName("Built-in connection")
      .setType(ConnectionType.FILE)
      .putProperty("k1", "v1")
      .build();
    ConnectionMeta manuallyCreatedMeta = ConnectionMeta.builder()
      .setName("Manually created connection")
      .setType(ConnectionType.FILE)
      .putProperty("k1", "v1")
      .build();

    NamespacedId preconfigured = run(connectionStore -> connectionStore.create(nsGen1, preconfiguredMeta, true));
    NamespacedId manuallyCreated = run(connectionStore -> connectionStore.create(nsGen1, manuallyCreatedMeta));

    Connection preconfiguredConnection =
      run(connectionStore -> connectionStore.get(new NamespacedId(nsGen1, preconfigured.getId())));
    Connection manuallyCreatedConnection =
      run(connectionStore -> connectionStore.get(new NamespacedId(nsGen1, manuallyCreated.getId())));

    // test that built-in connection returns that it is built-in
    Assert.assertTrue(preconfiguredConnection.isPreconfigured());

    // test that a manually created connection returns that it is not built-in
    Assert.assertFalse(manuallyCreatedConnection.isPreconfigured());
  }

  private void assertConnectionMetaEquality(ConnectionMeta expected, Connection actual) {
    // can't just compare objects since the store sets created, id, and updated fields
    Assert.assertEquals(expected.getName(), actual.getName());
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getProperties(), actual.getProperties());
  }

  private <T> T run(ConnectionStoreCallable<T> callable) {
    return TransactionRunners.run(getTransactionRunner(), context -> {
      ConnectionStore connectionStore = ConnectionStore.get(context);
      return callable.run(connectionStore);
    }, ConnectionNotFoundException.class, ConnectionAlreadyExistsException.class);
  }

  private interface ConnectionStoreCallable<T> {
    T run(ConnectionStore connectionStore) throws Exception;
  }
}
