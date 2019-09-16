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

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.wrangler.dataset.datamodel.PackageDataModelLoader;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModel;
import io.cdap.wrangler.proto.datamodel.JsonSchemaDataModelDescriptor;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
/**
 * Tests for {@link PackageDataModelLoader}.
 */
public class PackageDataModelLoaderTest extends SystemAppTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void testLoadSchema() {
    PackageDataModelLoader sl = new PackageDataModelLoader();
    try {
      Namespace namespace = new Namespace("_testLoadSchema_", 0L);
      Collection<JsonSchemaDataModelDescriptor> descriptors = sl.load(namespace);
      Assert.assertNotEquals(0, descriptors.size());
      for (JsonSchemaDataModelDescriptor descriptor : descriptors) {
        JsonSchemaDataModel def = descriptor.getDataModel();
        Assert.assertNotEquals(0, descriptor.getNamespacedId().getId().length());
        Assert.assertNotNull(def);
        Assert.assertNotNull(def.getDefinitions());
        Assert.assertNotNull(def.getDiscriminator());
        Assert.assertNotNull(def.getMetadata());
        Assert.assertNotNull(def.getDescription());
      }
    } catch (IOException e) {
      Assert.fail(String.format("failed to load, error: %s", e.getMessage()));
    }
  }
}
