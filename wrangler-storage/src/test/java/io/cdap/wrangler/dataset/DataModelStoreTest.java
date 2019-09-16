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

import com.google.gson.Gson;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.wrangler.dataset.datamodel.DataModelStore;
import io.cdap.wrangler.dataset.schema.SchemaFileNotFoundException;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.datamodel.DataModelInfo;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModel;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModel.MetaData;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModelDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link DataModelStore}
 */
public class DataModelStoreTest extends SystemAppTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);
  private static final String genericJsonSchema = "{\n"
      + " \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
      + " \"$id\": \"http://hl7.org/fhir/json-schema/4.0\",\n"
      + " \"description\": \"see http://hl7.org/fhir/json.html#schema for information\",\n"
      + " \"discriminator\": {\n"
      + "       \"propertyName\": \"resourceType\",\n"
      + "       \"mapping\": {}\n"
      + "   },\n"
      + " \"metadata\": {\n"
      + "       \"standard\": {\n"
      + "           \"name\": \"fhir\",\n"
      + "           \"version\": \"R4\"\n"
      + "        },\n"
      + "       \"version\": \"1.0.0\"\n"
      + "   },\n"
      + " \"oneOf\": [],\n"
      + " \"definitions\": {}\n"
      + "}\n";

  private static final String discriminatorJsonSchema = "{\n"
      + " \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
      + " \"$id\": \"http://hl7.org/fhir/json-schema/4.0\",\n"
      + " \"description\": \"see http://hl7.org/fhir/json.html#schema for information\",\n"
      + " \"discriminator\": {\n"
      + "       \"propertyName\": \"resourceType\",\n"
      + "       \"mapping\": {\n"
      + "             \"Account\": \"#/definitions/Account\"\n"
      + "         }\n"
      + "   },\n"
      + " \"metadata\": {\n"
      + "       \"standard\": {\n"
      + "           \"name\": \"fhir\",\n"
      + "           \"version\": \"R4\"\n"
      + "        },\n"
      + "       \"version\": \"1.0.1\"\n"
      + "   },\n"
      + " \"oneOf\": [],\n"
      + " \"definitions\": {\n"
      + "     \"Account\": {\n"
      + "         \"description\": \"A financial tool for tracking value accrued for a paricular purpose\",\n"
      + "         \"properties\": {\n"
      + "               \"resourceType\": {\n"
      + "                   \"description\": \"This is a Account resource\",\n"
      + "                   \"const\": \"Account\" \n"
      + "                 }\n"
      + "           }\n"
      + "       }\n"
      + "   }\n"
      + "}\n";

  private static final Gson GSON = new Gson();

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(DataModelStore.TABLE_SPEC);
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(DataModelStore.TABLE_SPEC.getTableId());
  }

  @Test
  public void testNotFoundExceptions() throws Exception {
    NamespacedId id = new NamespacedId(new Namespace("c0", 10L), "_testException_01");
    try {
      call(store -> store.readDataModelDescriptor(id));
      Assert.fail("Reading an non existent data model should fail.");
    } catch (SchemaFileNotFoundException e) {
      // expected
    }
    try {
      call(store -> store.listModels(id));
      Assert.fail("Listing models for a non existent data model should fail.");
    } catch (SchemaFileNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testReadWrite() {
    Namespace namespace = new Namespace("c0", 10L);
    Assert.assertTrue(call(store -> store.listDataModels(namespace).isEmpty()));
    Assert.assertTrue(call(store -> store.empty(namespace)));

    NamespacedId id = new NamespacedId(namespace, "_testReadWrite_01");
    JsonSchemaDataModelDescriptor expected = JsonSchemaDataModelDescriptor.builder()
        .setId(id)
        .setJsonSchemaDataModel(GSON.fromJson(genericJsonSchema, JsonSchemaDataModel.class))
        .build();
    run(store -> store.addOrUpdateDataModel(expected));
    Assert.assertTrue(call(store -> store.hasDataModel(id)));
    JsonSchemaDataModelDescriptor actual = call(store -> store.readDataModelDescriptor(id));
    Assert.assertEquals(expected, actual);

    JsonSchemaDataModelDescriptor updateExpected = JsonSchemaDataModelDescriptor.builder(expected)
        .setJsonSchemaDataModel(GSON.fromJson(discriminatorJsonSchema, JsonSchemaDataModel.class))
        .build();
    run(store -> store.addOrUpdateDataModel(updateExpected));
    actual = call(store -> store.readDataModelDescriptor(id));
    Assert.assertEquals(call(store -> store.listDataModels(namespace)).size(), 1);
    Assert.assertEquals(updateExpected.getDataModel().getDiscriminator(),
        actual.getDataModel().getDiscriminator());
    Assert.assertFalse(call(store -> store.empty(namespace)));

    // test read models
    JsonSchemaDataModel.Discriminator expectedDiscriminator = updateExpected.getDataModel().getDiscriminator();
    Set<String> expectedModels = expectedDiscriminator.getMapping().keySet();
    Set<String> actualModels = new HashSet<>(call(store -> store.listModels(id)));
    Assert.assertEquals(expectedModels, actualModels);

    // test write new
    NamespacedId id2 = new NamespacedId(namespace, "_testReadWrite_02");
    JsonSchemaDataModelDescriptor update2 = JsonSchemaDataModelDescriptor.builder(expected)
        .setId(id2)
        .build();
    Map<String, MetaData> expectedMeta = new HashMap<String, MetaData>() {{
      put(id.getId(), updateExpected.getDataModel().getMetadata());
      put(id2.getId(), update2.getDataModel().getMetadata());
    }};
    run(store -> store.addOrUpdateDataModel(update2));
    List<DataModelInfo<JsonSchemaDataModel.Standard>> updatedActual = call(store -> store.listDataModels(namespace));
    Assert.assertEquals(updatedActual.size(), 2);
    Assert.assertTrue(expectedMeta.containsKey(updatedActual.get(0).getId()));
    Assert.assertTrue(expectedMeta.containsKey(updatedActual.get(1).getId()));

    run(store -> store.deleteDataModel(id));
    run(store -> store.deleteDataModel(id2));
    Assert.assertTrue(call(store -> store.listDataModels(namespace).isEmpty()));
    Assert.assertTrue(call(store -> store.empty(namespace)));
  }

  @Test
  public void testListDataModels() {
    Namespace namespace = new Namespace("c0", 10L);
    Assert.assertTrue(call(store -> store.listDataModels(namespace).isEmpty()));

    NamespacedId id = new NamespacedId(namespace, "_testListDataModels_01");
    JsonSchemaDataModelDescriptor descriptor = JsonSchemaDataModelDescriptor.builder()
        .setId(id)
        .setJsonSchemaDataModel(GSON.fromJson(genericJsonSchema, JsonSchemaDataModel.class))
        .build();
    run(store -> store.addOrUpdateDataModel(descriptor));

    JsonSchemaDataModel.MetaData expected = descriptor.getDataModel().getMetadata();
    List<DataModelInfo<JsonSchemaDataModel.Standard>> actual =
        (List<DataModelInfo<JsonSchemaDataModel.Standard>>) call(store -> store.listDataModels(namespace));

    Assert.assertEquals(1, actual.size());
    Assert.assertEquals(descriptor.getNamespacedId().getId(), actual.get(0).getId());
    Assert.assertEquals(expected.getVersion(), actual.get(0).getVersion());
    Assert.assertEquals(expected.getStandard(), actual.get(0).getStandard());
  }

  @Test
  public void testDelete() {
    Namespace namespace = new Namespace("c0", 10L);
    Assert.assertTrue(call(store -> store.listDataModels(namespace).isEmpty()));

    NamespacedId id = new NamespacedId(namespace, "_testDelete_01");
    JsonSchemaDataModelDescriptor descriptor = JsonSchemaDataModelDescriptor.builder()
        .setId(id)
        .setJsonSchemaDataModel(GSON.fromJson(genericJsonSchema, JsonSchemaDataModel.class))
        .build();
    run(store -> store.addOrUpdateDataModel(descriptor));
    Assert.assertEquals(call(store -> store.listDataModels(namespace)).size(), 1);

    // test delete
    run(store -> store.deleteDataModel(id));
    Assert.assertTrue(call(store -> store.listDataModels(namespace).isEmpty()));
    Assert.assertFalse(call(store -> store.hasDataModel(id)));
    try {
      call(store -> store.readDataModelDescriptor(id));
      Assert.fail("schema definition was not deleted.");
    } catch (SchemaFileNotFoundException e) {
      // nothing present
    }

    // test delete non existent id
    run(store -> store.deleteDataModel(id));
    Assert.assertTrue(call(store -> store.listDataModels(namespace).isEmpty()));
    Assert.assertTrue(call(store -> store.empty(namespace)));
  }

  @Test
  public void testListModels() {
    Namespace namespace = new Namespace("c0", 10L);
    Assert.assertTrue(call(store -> store.listDataModels(namespace).isEmpty()));

    NamespacedId id = new NamespacedId(namespace, "_testListModels_01");
    JsonSchemaDataModelDescriptor descriptor = JsonSchemaDataModelDescriptor.builder()
        .setId(id)
        .setJsonSchemaDataModel(GSON.fromJson(discriminatorJsonSchema, JsonSchemaDataModel.class))
        .build();
    run(store -> store.addOrUpdateDataModel(descriptor));
    Assert.assertEquals(call(store -> store.listDataModels(namespace)).size(), 1);
    Assert.assertEquals(call(store -> store.listModels(id)).size(), 1);
    Assert.assertEquals(call(store -> store.listModels(id)).get(0), "Account");

    run(store -> store.deleteDataModel(id));
  }

  private <T> T call(SchemaDefinitionStoreCallable<T> callable) {
    return TransactionRunners.run(getTransactionRunner(), context -> {
      DataModelStore store = DataModelStore.get(context);
      return callable.run(store);
    }, SchemaFileNotFoundException.class);
  }

  private void run(SchemaDefinitionStoreRunnable runnable) {
    TransactionRunners.run(getTransactionRunner(), context -> {
      DataModelStore store = DataModelStore.get(context);
      runnable.run(store);
    }, SchemaFileNotFoundException.class);
  }

  private interface SchemaDefinitionStoreRunnable {
    void run(DataModelStore sds) throws Exception;
  }

  private interface SchemaDefinitionStoreCallable<T> {
    T run(DataModelStore sds) throws Exception;
  }
}
