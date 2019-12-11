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

import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import java.io.IOException;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class PackagedSchemaLoaderTest {

  @Test
  public void testPackageSchemaLoader_omopSchema_successfulLoad() throws Exception {
    PackagedSchemaLoader loader = new PackagedSchemaLoader(
        PackagedSchemaLoaderTest.class.getClassLoader(), "schemas");
    NamespacedId id = new NamespacedId(new Namespace("test", 0L), "omop_6.0.0");
    Schema schema = loader.load(id);
    Assert.assertEquals("OMOP_6.0.0", schema.getFullName());
  }

  @Test(expected = IOException.class)
  public void testPackageSchemaLoader_missingSchema_throwsException() throws Exception {
    PackagedSchemaLoader loader = new PackagedSchemaLoader(
        PackagedSchemaLoaderTest.class.getClassLoader(), "schemas");
    NamespacedId id = new NamespacedId(new Namespace("test", 0L), "_missing_");
    loader.load(id);
  }
}
