/*
 *  Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.validation;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.utils.Manifest;
import io.cdap.wrangler.utils.Manifest.Standard;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for ValidateStandard and the manifest and schemas in the package.
 */
public class ValidateStandardTest {

  private static Map<String, Standard> getSpecsInArchive()
    throws IOException, NoSuchAlgorithmException {
    Map<String, Standard> schemas = new HashMap<>();
    CodeSource src = ValidateStandard.class.getProtectionDomain().getCodeSource();
    if (src != null) {
      File schemasRoot =
        Paths.get(src.getLocation().getPath(), ValidateStandard.SCHEMAS_RESOURCE_PATH).toFile();

      if (!schemasRoot.isDirectory()) {
        throw new IOException(
          String.format("Schemas root %s was not a directory", schemasRoot.getPath()));
      }

      for (File f : schemasRoot.listFiles()) {
        if (f.toPath().endsWith(ValidateStandard.MANIFEST_PATH)) {
          continue;
        }

        String hash = calcHash(new FileInputStream(f));
        schemas.put(
          FilenameUtils.getBaseName(f.getName()),
          new Standard(hash, FilenameUtils.getExtension(f.getName())));
      }
    }

    return schemas;
  }

  private static String calcHash(InputStream is) throws IOException, NoSuchAlgorithmException {
    byte[] bytes = IOUtils.toByteArray(is);
    MessageDigest d = MessageDigest.getInstance("SHA-256");
    byte[] hash = d.digest(bytes);

    Formatter f = new Formatter();
    for (byte b : hash) {
      f.format("%02x", b);
    }
    return f.toString();
  }

  private static InputStream readResource(String name) throws IOException {
    InputStream resourceStream = ValidateStandard.class.getClassLoader().getResourceAsStream(name);

    if (resourceStream == null) {
      throw new IOException(String.format("Can't read/find resource %s", name));
    }

    return resourceStream;
  }

  @Test
  public void testValidation() throws Exception {
    JsonObject badJson =
      new Gson()
        .fromJson("{\"resourceType\": \"Patient\", \"active\": \"meow\"}", JsonObject.class);
    JsonObject goodJson =
      new Gson()
        .fromJson(
          "{\"resourceType\": \"Patient\", \"active\": true, \"gender\": \"female\"}",
          JsonObject.class);

    String[] directives = new String[]{
      "validate-standard :col1 hl7-fhir-r4",
    };

    List<Row> rows = Arrays.asList(
      new Row("col1", badJson),
      new Row("col1", goodJson)
    );

    List<Row> actual = TestingRig.execute(directives, rows);

    assertEquals(1, actual.size());
    assertEquals(goodJson, actual.get(0).getValue(0));
  }

  /**
   * This test verifies that the manifest in the resources matches up with both the actual schemas in the resources as
   * well as the implementations provided to handle those schemas.
   */
  @Test
  public void verifyManifest() throws Exception {
    InputStream manifestStream = readResource(ValidateStandard.MANIFEST_PATH);
    Manifest manifest =
      new Gson().getAdapter(Manifest.class).fromJson(new InputStreamReader(manifestStream));

    Map<String, Standard> declaredSpecs = manifest.getStandards();
    Map<String, Standard> actualSpecs = getSpecsInArchive();

    assertEquals(
      "Manifest contains different number of specs than there are in the artifact",
      declaredSpecs.size(),
      actualSpecs.size());

    for (String spec : declaredSpecs.keySet()) {
      assertTrue(
        String.format("Manifest had spec %s but the artifact did not", spec),
        actualSpecs.containsKey(spec));

      Standard declared = declaredSpecs.get(spec);
      Standard actual = actualSpecs.get(spec);

      assertEquals(
        String.format(
          "Declared standard %s did not match actual %s",
          declared.toString(), actual.toString()),
        declared,
        actual);

      assertTrue(
        String.format(
          "Standard %s does not have a handler/factory registered in %s",
          spec, ValidateStandard.class.getName()),
        ValidateStandard.FORMAT_TO_FACTORY.containsKey(actual.getFormat()));
    }
  }
}
