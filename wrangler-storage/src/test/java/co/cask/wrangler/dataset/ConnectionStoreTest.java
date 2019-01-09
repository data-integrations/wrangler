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

    try {
      connectionStore.get("id");
      Assert.fail("Getting a non-existent connection did not throw an exception.");
    } catch (ConnectionNotFoundException e) {
      // expected
    }

    try {
      connectionStore.update("id", ConnectionMeta.builder().setName("name").setType(ConnectionType.FILE).build());
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
    connectionStore.create(meta);
    try {
      connectionStore.create(meta);
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

    Assert.assertTrue(connectionStore.list(x -> true).isEmpty());

    // test creation
    ConnectionMeta expected = ConnectionMeta.builder()
      .setName("My Connection")
      .setType(ConnectionType.FILE)
      .putProperty("k1", "v1")
      .build();
    String id = connectionStore.create(expected);
    tableManager.flush();

    Connection actual = connectionStore.get(id);
    // can't just compare objects since the store sets created, id, and updated fields
    Assert.assertEquals(expected.getName(), actual.getName());
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getProperties(), actual.getProperties());

    // test update
    ConnectionMeta updated = ConnectionMeta.builder()
      .setName(expected.getName())
      .setType(expected.getType())
      .putProperty("k1", "v2")
      .build();
    connectionStore.update(id, updated);
    tableManager.flush();
    actual = connectionStore.get(id);
    Assert.assertEquals(updated.getName(), actual.getName());
    Assert.assertEquals(updated.getType(), actual.getType());
    Assert.assertEquals(updated.getProperties(), actual.getProperties());

    // test scan with non-matching filter
    Assert.assertTrue(connectionStore.list(c -> c.getType().equals(ConnectionType.DATABASE.getType())).isEmpty());
    // text scan
    List<Connection> list = connectionStore.list(x -> true);
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(updated.getName(), actual.getName());
    Assert.assertEquals(updated.getType(), actual.getType());
    Assert.assertEquals(updated.getProperties(), actual.getProperties());

    // test delete
    connectionStore.delete(id);
    tableManager.flush();
    try {
      connectionStore.get(id);
      Assert.fail("Connection was not properly deleted.");
    } catch (ConnectionNotFoundException e) {
      // expected
    }
    Assert.assertTrue(connectionStore.list(x -> true).isEmpty());
  }
}
