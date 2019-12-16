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
package io.cdap.wrangler.utils;

import org.apache.avro.Schema;
import org.apache.commons.collections4.SetValuedMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class PackagedSchemaLoaderTest {

  @Test
  public void testPackageSchemaLoader_omopSchema_successfulLoad() throws Exception {
    PackagedSchemaLoader loader = new PackagedSchemaLoader(
      PackagedSchemaLoaderTest.class.getClassLoader(), "schemas", "manifest.json");
    SetValuedMap<String, Schema> schemas = loader.load();
    Assert.assertNotNull(schemas.get("google.com.datamodels.omop.OMOP_6_0_0"));
  }

  @Test(expected = IOException.class)
  public void testPackageSchemaLoader_missingClassloader_throwsException() throws Exception {
    PackagedSchemaLoader loader = new PackagedSchemaLoader(null, "schemas", "manifest.json");
    loader.load();
  }

  @Test(expected = IOException.class)
  public void testPackageSchemaLoader_invalidDirectory_throwsException() throws Exception {
    PackagedSchemaLoader loader = new PackagedSchemaLoader(
      PackagedSchemaLoaderTest.class.getClassLoader(), "__missing__", "manifest.json");
    loader.load();
  }

  @Test(expected = IOException.class)
  public void testPackageSchemaLoader_missingManifest_throwsException() throws Exception {
    PackagedSchemaLoader loader = new PackagedSchemaLoader(
      PackagedSchemaLoaderTest.class.getClassLoader(), "schemas", "");
    loader.load();
  }
}
