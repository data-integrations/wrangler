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
import co.cask.wrangler.dataset.schema.SchemaDescriptorType;
import co.cask.wrangler.dataset.schema.SchemaEntry;
import co.cask.wrangler.dataset.schema.SchemaRegistry;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link SchemaRegistry}.
 */
public class SchemaRegistryTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void testCRUD() throws Exception {
    addDatasetInstance("table", "crudtest");
    DataSetManager<Table> tableManager = getDataset("crudtest");
    Table table = tableManager.get();
    SchemaRegistry registry = new SchemaRegistry(table);

    String id = "id0";
    SchemaEntry expected = new SchemaEntry(id, "some name", "desc", SchemaDescriptorType.AVRO, Collections.emptySet(),
                                           null, 1L);
    Assert.assertFalse(registry.hasSchema(id));

    // test schema creation
    registry.create(id, expected.getName(), expected.getDescription(), expected.getType());
    tableManager.flush();
    Assert.assertTrue(registry.hasSchema(id));
    Assert.assertFalse(registry.hasSchema(id, 5L));
    SchemaEntry actual = registry.get(id);
    Assert.assertEquals(expected, actual);
    
    // test adding a schema entry
    byte[] spec = new byte[] { 0, 1, 2 };
    long version1 = registry.add(id, spec);
    tableManager.flush();
    Set<Long> expectedVersions = new HashSet<>();
    expectedVersions.add(version1);
    expected = new SchemaEntry(id, expected.getName(), expected.getDescription(), expected.getType(),
                               expectedVersions, spec, version1);
    SchemaEntry v1Actual = registry.get(id);
    Assert.assertTrue(registry.hasSchema(id, version1));
    Assert.assertEquals(expected, v1Actual);
    v1Actual = registry.get(id, version1);
    Assert.assertEquals(expected, v1Actual);
    Assert.assertEquals(expectedVersions, registry.getVersions(id));

    // test get versions
    spec = new byte[] { 3, 4, 5 };
    long version2 = registry.add(id, spec);
    tableManager.flush();
    expectedVersions.clear();
    expectedVersions.add(version1);
    expectedVersions.add(version2);
    expected = new SchemaEntry(id, expected.getName(), expected.getDescription(), expected.getType(),
                               expectedVersions, spec, version2);
    Assert.assertEquals(expectedVersions, registry.getVersions(id));

    // test version specific deletion
    registry.remove(id, version1);
    tableManager.flush();
    Assert.assertFalse(registry.hasSchema(id, version1));
    Assert.assertTrue(registry.hasSchema(id, version2));
    Assert.assertEquals(Collections.singleton(version2), registry.getVersions(id));
    
    // test deleting all entries still keeps the schema around
    registry.remove(id, version2);
    tableManager.flush();
    Assert.assertFalse(registry.hasSchema(id, version2));
    Assert.assertTrue(registry.hasSchema(id));

    // test deleting the schema
    long version3 = registry.add(id, spec);
    tableManager.flush();
    registry.delete(id);
    tableManager.flush();
    Assert.assertFalse(registry.hasSchema(id));
    Assert.assertFalse(registry.hasSchema(id, version3));
  }

}
