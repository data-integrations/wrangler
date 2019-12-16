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

import com.google.gson.Gson;
import io.cdap.wrangler.utils.Manifest.Standard;
import org.apache.avro.Schema;
import org.apache.commons.collections4.SetValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Map;

/**
 * PackagedSchemaLoader loads an AVRO data model schema bundled as a resource within a package.
 */
public class PackagedSchemaLoader implements AvroSchemaLoader {

  private final ClassLoader classLoader;
  private final String manifestPath;
  private final String resourceDirectory;
  private static final String FORMAT = "avsc";

  public PackagedSchemaLoader(ClassLoader classLoader, String resourceDirectory, String manifestPath) {
    this.classLoader = classLoader;
    this.resourceDirectory = resourceDirectory;
    this.manifestPath = manifestPath;
  }

  /**
   * Loads an AVRO schema definition packaged as a resource within the packaged defined by the class loader.
   *
   * @return a map with keys representing the name of the schema. The value is a set of all of the revisions of the
   * @throws IOException when resource missing or parsing error.
   */
  @Override
  public SetValuedMap<String, Schema> load() throws IOException {
    if (classLoader == null) {
      throw new IOException("unable to load resource due to null ClassLoader");
    }
    Manifest manifest = getManifest();
    return loadSchemas(manifest);
  }

  private Manifest getManifest() throws IOException {
    InputStream manifestStream = loadResource(manifestPath);
    return new Gson().getAdapter(Manifest.class).fromJson(new InputStreamReader(manifestStream));
  }

  private SetValuedMap<String, Schema> loadSchemas(Manifest manifest) throws IOException {
    SetValuedMap<String, Schema> avroSchemas = new HashSetValuedHashMap<>();
    if (manifest.getStandards() == null) {
      return avroSchemas;
    }
    Schema.Parser parser = new Schema.Parser().setValidate(false);
    for (Map.Entry<String, Standard> spec : manifest.getStandards().entrySet()) {
      if (spec.getValue().getFormat().equals(PackagedSchemaLoader.FORMAT)) {
        InputStream schemaStream = loadResource(String.format("%s.%s", spec.getKey(),
                                                              PackagedSchemaLoader.FORMAT));
        Schema schema = parser.parse(schemaStream);
        if (schema != null) {
          avroSchemas.put(schema.getFullName(), schema);
        }
      }
    }
    return avroSchemas;
  }

  private InputStream loadResource(String name) throws IOException {
    String filePath = Paths.get(resourceDirectory, name).toString();
    InputStream inputStream = this.classLoader.getResourceAsStream(filePath);
    if (inputStream == null) {
      throw new IOException(String.format("Can't read resource %s", filePath));
    }
    return inputStream;
  }
}
