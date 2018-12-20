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
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.connections.ConnectionType;
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
  public void testCRUD() throws Exception {
    addDatasetInstance("table", "crudtest");
    DataSetManager<Table> tableManager = getDataset("crudtest");
    Table table = tableManager.get();
    ConnectionStore connectionStore = new ConnectionStore(table);

    Assert.assertTrue(connectionStore.scan(x -> true).isEmpty());

    // test creation
    Connection expected = new Connection();
    expected.setName("My Connection");
    expected.setType(ConnectionType.FILE);
    expected.putProp("k1", "v1");
    String id = connectionStore.create(expected);
    tableManager.flush();

    Connection actual = connectionStore.get(id);
    // can't just compare objects since the store sets created, id, and updated fields
    Assert.assertEquals(expected.getName(), actual.getName());
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getAllProps(), actual.getAllProps());

    // test update
    Connection updated = new Connection();
    updated.setName(expected.getName());
    updated.setType(expected.getType());
    updated.putProp("k1", "v2");
    connectionStore.update(id, updated);
    tableManager.flush();
    actual = connectionStore.get(id);
    Assert.assertEquals(updated.getName(), actual.getName());
    Assert.assertEquals(updated.getType(), actual.getType());
    Assert.assertEquals(updated.getAllProps(), actual.getAllProps());

    // test scan with non-matching filter
    Assert.assertTrue(connectionStore.scan(c -> c.getType().equals(ConnectionType.DATABASE.getType())).isEmpty());
    // text scan
    List<Connection> list = connectionStore.scan(x -> true);
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(updated.getName(), actual.getName());
    Assert.assertEquals(updated.getType(), actual.getType());
    Assert.assertEquals(updated.getAllProps(), actual.getAllProps());

    // test delete
    connectionStore.delete(id);
    tableManager.flush();
    Assert.assertNull(connectionStore.get(id));
    Assert.assertTrue(connectionStore.scan(x -> true).isEmpty());
  }
}
