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

import io.cdap.wrangler.proto.NamespacedId;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.avro.Schema;

import java.io.InputStream;

/**
 * PackagedSchemaLoader loads an AVRO schema bundled as a resource within a package.
 */
public class PackagedSchemaLoader implements AvroSchemaLoader {
  private final ClassLoader classLoader;
  private final String directory;
  private static final String EXTENSION = "avsc";

  public PackagedSchemaLoader(ClassLoader classLoader, String directory) {
    this.classLoader = classLoader;
    this.directory = directory;
  }

  /**
   * Loads the AVRO schema definition packaged as a resource within the packaged defined by the
   * class loader.
   * @param namespacedId identifier of the schema to load.
   * @return parsed {@link Schema} schema
   * @throws IOException when resource missing or parsing error.
   */
  @Override
  public Schema load(NamespacedId namespacedId) throws IOException {
    Path path = Paths.get(directory, String.format("%s.%s", namespacedId.getId(), EXTENSION));
    InputStream schemaStream = this.classLoader.getResourceAsStream(path.toString());
    if (schemaStream == null) {
      throw new IOException(String.format("Can't read/find resource %s", namespacedId.getId()));
    }
      Schema.Parser parser = new Schema.Parser().setValidate(false);
      return parser.parse(schemaStream);
  }
}
