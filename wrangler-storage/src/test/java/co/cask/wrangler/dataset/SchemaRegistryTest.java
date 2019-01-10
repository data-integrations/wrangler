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
import co.cask.wrangler.dataset.schema.SchemaDescriptor;
import co.cask.wrangler.dataset.schema.SchemaNotFoundException;
import co.cask.wrangler.dataset.schema.SchemaRegistry;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.schema.SchemaDescriptorType;
import co.cask.wrangler.proto.schema.SchemaEntry;
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
  public void testNotFoundExceptions() throws Exception {
    addDatasetInstance("table", "notfoundtest");
    DataSetManager<Table> tableManager = getDataset("notfoundtest");
    Table table = tableManager.get();
    SchemaRegistry registry = new SchemaRegistry(table);

    NamespacedId id = NamespacedId.of("c0", "id0");
    try {
      registry.remove(id, 0L);
      Assert.fail("removing an entry from a non-existent schema did not throw an exception");
    } catch (SchemaNotFoundException e) {
      // expected
    }

    try {
      registry.getEntry(id);
      Assert.fail("getting a non-existent schema did not throw an exception");
    } catch (SchemaNotFoundException e) {
      // expected
    }

    try {
      registry.getVersions(id);
      Assert.fail("getting versions of a non-existent schema did not throw an exception");
    } catch (SchemaNotFoundException e) {
      // expected
    }

    SchemaDescriptor descriptor = new SchemaDescriptor(id, "some name", "desc", SchemaDescriptorType.AVRO);
    registry.write(descriptor);
    tableManager.flush();

    try {
      registry.getEntry(id, 1L);
      Assert.fail("getting a non-existent schema entry did not throw an exception");
    } catch (SchemaNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testCRUD() throws Exception {
    addDatasetInstance("table", "crudtest");
    DataSetManager<Table> tableManager = getDataset("crudtest");
    Table table = tableManager.get();
    SchemaRegistry registry = new SchemaRegistry(table);

    NamespacedId id = NamespacedId.of("c0", "id0");
    Assert.assertFalse(registry.hasSchema(id));

    // test schema creation
    SchemaDescriptor descriptor = new SchemaDescriptor(id, "some name", "desc", SchemaDescriptorType.AVRO);
    SchemaEntry expected = new SchemaEntry(id, descriptor.getName(), descriptor.getDescription(),
                                           descriptor.getType(), Collections.emptySet(), null, null);
    registry.write(descriptor);
    tableManager.flush();
    Assert.assertTrue(registry.hasSchema(id));
    Assert.assertFalse(registry.hasSchema(id, 5L));
    SchemaEntry actual = registry.getEntry(id);
    Assert.assertEquals(expected, actual);
    
    // test adding a schema entry
    byte[] spec = new byte[] { 0, 1, 2 };
    long version1 = registry.add(id, spec);
    tableManager.flush();
    Set<Long> expectedVersions = new HashSet<>();
    expectedVersions.add(version1);
    expected = new SchemaEntry(id, expected.getName(), expected.getDescription(), expected.getType(),
                               expectedVersions, spec, version1);
    SchemaEntry v1Actual = registry.getEntry(id);
    Assert.assertTrue(registry.hasSchema(id, version1));
    Assert.assertEquals(expected, v1Actual);
    v1Actual = registry.getEntry(id, version1);
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
    Assert.assertEquals(expected, registry.getEntry(id, version2));
    Assert.assertEquals(expected, registry.getEntry(id));

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
    registry.add(id, spec);
    tableManager.flush();
    registry.delete(id);
    tableManager.flush();
    Assert.assertFalse(registry.hasSchema(id));
  }

  @Test
  public void testNamespaceIsolation() throws Exception {
    addDatasetInstance("table", "nsTest");
    DataSetManager<Table> tableManager = getDataset("nsTest");
    Table table = tableManager.get();
    SchemaRegistry registry = new SchemaRegistry(table);

    String context1 = "c1";
    String context2 = "c2";

    NamespacedId id1 = NamespacedId.of(context1, "id0");
    NamespacedId id2 = NamespacedId.of(context2, id1.getId());

    SchemaDescriptor descriptor1 = new SchemaDescriptor(id1, "name1", "desc1", SchemaDescriptorType.AVRO);
    SchemaEntry expected1 = new SchemaEntry(id1, descriptor1.getName(), descriptor1.getDescription(),
                                            descriptor1.getType(), Collections.emptySet(), null, null);
    SchemaDescriptor descriptor2 = new SchemaDescriptor(id2, "name2", "desc2", SchemaDescriptorType.AVRO);
    SchemaEntry expected2 = new SchemaEntry(id2, descriptor2.getName(), descriptor2.getDescription(),
                                            descriptor2.getType(), Collections.emptySet(), null, null);

    // test writes don't interfere with each other
    registry.write(descriptor1);
    registry.write(descriptor2);
    tableManager.flush();
    Assert.assertEquals(expected1, registry.getEntry(id1));
    Assert.assertEquals(expected2, registry.getEntry(id2));

    // test version lists don't overlap
    long v1 = registry.add(id1, new byte[] { 1 });
    long v2 = registry.add(id2, new byte[] { 2 });
    tableManager.flush();
    Assert.assertEquals(Collections.singleton(v1), registry.getVersions(id1));
    Assert.assertEquals(Collections.singleton(v2), registry.getVersions(id2));

    // test delete doesn't affect schema in another context
    registry.delete(id1);
    tableManager.flush();
    try {
      registry.getVersions(id1);
    } catch (SchemaNotFoundException e) {
      // expected
    }
    Assert.assertEquals(Collections.singleton(v2), registry.getVersions(id2));
  }
}
