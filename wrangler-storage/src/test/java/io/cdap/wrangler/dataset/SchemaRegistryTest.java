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
import io.cdap.wrangler.dataset.schema.SchemaDescriptor;
import io.cdap.wrangler.dataset.schema.SchemaNotFoundException;
import io.cdap.wrangler.dataset.schema.SchemaRegistry;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.schema.SchemaDescriptorType;
import io.cdap.wrangler.proto.schema.SchemaEntry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link SchemaRegistry}.
 */
public class SchemaRegistryTest extends SystemAppTestBase {

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(SchemaRegistry.META_TABLE_SPEC);
    getStructuredTableAdmin().create(SchemaRegistry.ENTRY_TABLE_SPEC);
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(SchemaRegistry.META_TABLE_SPEC.getTableId());
    getStructuredTableAdmin().drop(SchemaRegistry.ENTRY_TABLE_SPEC.getTableId());
  }

  @Test
  public void testNotFoundExceptions() throws Exception {
    getTransactionRunner().run(context -> {
      SchemaRegistry registry = SchemaRegistry.get(context);
      NamespacedId id = new NamespacedId(new Namespace("c0", 10L), "id0");
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

      try {
        registry.getEntry(id, 1L);
        Assert.fail("getting a non-existent schema entry did not throw an exception");
      } catch (SchemaNotFoundException e) {
        // expected
      }
    });
  }

  @Test
  public void testCRUD() {
    NamespacedId id = new NamespacedId(new Namespace("c0", 10L), "id0");
    Assert.assertFalse(call(registry -> registry.hasSchema(id)));

    // test schema creation
    SchemaDescriptor descriptor = new SchemaDescriptor(id, "some name", "desc", SchemaDescriptorType.AVRO);
    SchemaEntry expected = new SchemaEntry(id, descriptor.getName(), descriptor.getDescription(),
                                           descriptor.getType(), Collections.emptySet(), null, null);
    run(registry -> registry.write(descriptor));
    Assert.assertTrue(call(registry -> registry.hasSchema(id)));
    Assert.assertFalse(call(registry -> registry.hasSchema(id, 5L)));
    SchemaEntry actual = call(registry -> registry.getEntry(id));
    Assert.assertEquals(expected, actual);

    // test adding a schema entry
    byte[] spec1 = new byte[]{0, 1, 2};
    long version1 = call(registry -> registry.add(id, spec1));
    Set<Long> expectedVersions = new HashSet<>();
    expectedVersions.add(version1);
    expected = new SchemaEntry(id, expected.getName(), expected.getDescription(), expected.getType(),
                               expectedVersions, spec1, version1);
    SchemaEntry v1Actual = call(registry -> registry.getEntry(id));
    Assert.assertTrue(call(registry -> registry.hasSchema(id, version1)));
    Assert.assertEquals(expected, v1Actual);
    v1Actual = call(registry -> registry.getEntry(id, version1));
    Assert.assertEquals(expected, v1Actual);
    Assert.assertEquals(expectedVersions, call(registry -> registry.getVersions(id)));

    // test get versions
    byte[] spec2 = new byte[]{3, 4, 5};
    long version2 = call(registry -> registry.add(id, spec2));
    expectedVersions.clear();
    expectedVersions.add(version1);
    expectedVersions.add(version2);
    expected = new SchemaEntry(id, expected.getName(), expected.getDescription(), expected.getType(),
                               expectedVersions, spec2, version2);
    Assert.assertEquals(expectedVersions, call(registry -> registry.getVersions(id)));
    Assert.assertEquals(expected, call(registry -> registry.getEntry(id, version2)));
    Assert.assertEquals(expected, call(registry -> registry.getEntry(id)));

    // test version specific deletion
    run(registry -> registry.remove(id, version1));
    Assert.assertFalse(call(registry -> registry.hasSchema(id, version1)));
    Assert.assertTrue(call(registry -> registry.hasSchema(id, version2)));
    Assert.assertEquals(Collections.singleton(version2), call(registry -> registry.getVersions(id)));

    // test deleting all entries still keeps the schema around
    run(registry -> registry.remove(id, version2));
    Assert.assertFalse(call(registry -> registry.hasSchema(id, version2)));
    Assert.assertTrue(call(registry -> registry.hasSchema(id)));

    // test deleting the schema
    run(registry -> registry.add(id, spec2));
    run(registry -> registry.delete(id));
    Assert.assertFalse(call(registry -> registry.hasSchema(id)));
  }

  @Test
  public void testNamespaceIsolation() {
    Namespace ns1 = new Namespace("c1", 10L);
    Namespace ns2 = new Namespace("c2", 10L);

    NamespacedId id1 = new NamespacedId(ns1, "id0");
    NamespacedId id2 = new NamespacedId(ns2, id1.getId());

    SchemaDescriptor descriptor1 = new SchemaDescriptor(id1, "name1", "desc1", SchemaDescriptorType.AVRO);
    SchemaEntry expected1 = new SchemaEntry(id1, descriptor1.getName(), descriptor1.getDescription(),
                                            descriptor1.getType(), Collections.emptySet(), null, null);
    SchemaDescriptor descriptor2 = new SchemaDescriptor(id2, "name2", "desc2", SchemaDescriptorType.AVRO);
    SchemaEntry expected2 = new SchemaEntry(id2, descriptor2.getName(), descriptor2.getDescription(),
                                            descriptor2.getType(), Collections.emptySet(), null, null);

    // test writes don't interfere with each other
    run(registry -> registry.write(descriptor1));
    run(registry -> registry.write(descriptor2));
    Assert.assertEquals(expected1, call(registry -> registry.getEntry(id1)));
    Assert.assertEquals(expected2, call(registry -> registry.getEntry(id2)));

    // test version lists don't overlap
    long v1 = call(registry -> registry.add(id1, new byte[]{1}));
    long v2 = call(registry -> registry.add(id2, new byte[]{2}));
    Assert.assertEquals(Collections.singleton(v1), call(registry -> registry.getVersions(id1)));
    Assert.assertEquals(Collections.singleton(v2), call(registry -> registry.getVersions(id2)));

    // test delete doesn't affect schema in another context
    run(registry -> registry.delete(id1));
    try {
      run(registry -> registry.getVersions(id1));
    } catch (SchemaNotFoundException e) {
      // expected
    }
    Assert.assertEquals(Collections.singleton(v2), call(registry -> registry.getVersions(id2)));
  }

  @Test
  public void testNamespaceGenerations() {
    Namespace nsGen1 = new Namespace("ns1", 1L);
    Namespace nsGen2 = new Namespace("ns1", 2L);

    NamespacedId id = new NamespacedId(nsGen1, "id0");
    SchemaDescriptor descriptor = new SchemaDescriptor(id, "name", "desc", SchemaDescriptorType.AVRO);
    run(registry -> registry.write(descriptor));
    long v1 = call(registry -> registry.add(id, new byte[]{1}));

    // test that fetching with a different generation doesn't include the connection
    try {
      run(registry -> registry.getEntry(new NamespacedId(nsGen2, id.getId())));
      Assert.fail("schema with a different generation should not be visible.");
    } catch (SchemaNotFoundException e) {
      // expected
    }

    try {
      run(registry -> registry.getVersions(new NamespacedId(nsGen2, id.getId())));
      Assert.fail("schema entry with a different generation should not be visible.");
    } catch (SchemaNotFoundException e) {
      // expected
    }
  }

  private <T> T call(SchemaRegistryCallable<T> callable) {
    return TransactionRunners.run(getTransactionRunner(), context -> {
      SchemaRegistry registry = SchemaRegistry.get(context);
      return callable.run(registry);
    }, SchemaNotFoundException.class);
  }

  private void run(SchemaRegistryRunnable runnable) {
    TransactionRunners.run(getTransactionRunner(), context -> {
      SchemaRegistry registry = SchemaRegistry.get(context);
      runnable.run(registry);
    }, SchemaNotFoundException.class);
  }

  private interface SchemaRegistryRunnable {
    void run(SchemaRegistry registry) throws Exception;
  }

  private interface SchemaRegistryCallable<T> {
    T run(SchemaRegistry registry) throws Exception;
  }
}
