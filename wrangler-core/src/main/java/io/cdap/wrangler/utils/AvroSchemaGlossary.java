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
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The {@link AvroSchemaGlossary} class is used for management of AVRO data model schema definitions.
 */
public class AvroSchemaGlossary {

  private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaGlossary.class);
  private static final String REVISION_PROPERTY = "_revision";

  private AvroSchemaLoader avroSchemaLoader;
  private SetValuedMap<String, Schema> glossary = new HashSetValuedHashMap<>();

  public AvroSchemaGlossary(AvroSchemaLoader avroSchemaLoader) {
    this.avroSchemaLoader = avroSchemaLoader;
  }

  /**
   * Setter for the {@link AvroSchemaLoader} to use when populating the glossary.
   *
   * @param avroSchemaLoader the loader used to populate the glossary.
   */
  public void setAvroSchemaLoader(AvroSchemaLoader avroSchemaLoader) {
    this.avroSchemaLoader = avroSchemaLoader;
  }
  /**
   * Configures the {@link AvroSchemaGlossary} with the schemas accessible through the {@link AvroSchemaLoader}.
   *
   * @return true if successfully configure, otherwise false.
   */
  public boolean configure() {
    try {
      glossary = avroSchemaLoader.load();
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Retrieves the {@link Schema} from the glossary.
   *
   * @param name the name of the schema to retrieve.
   * @param revision the revision of the schema to retrieve.
   * @return {@link Schema} if found, otherwise null.
   */
  public Schema get(String name, long revision) {
    Collection<Schema> schemas = glossary.get(name);
    Schema result = null;
    for (Schema schema : schemas) {
      try {
        long rev = Long.parseLong(schema.getProp(AvroSchemaGlossary.REVISION_PROPERTY), 10);
        if (rev == revision) {
          result = schema;
          break;
        }
      } catch (NumberFormatException e) {
        LOG.error(String.format("unable to parse %s property within schema %s", AvroSchemaGlossary.REVISION_PROPERTY,
                                schema.getFullName()));
      }
    }
    return result;
  }

  /**
   * Retrieves all of the {@link Schema} contained within the glossary.
   *
   * @return a collection of all {@link Schema}
   */
  public Collection<Schema> getAll() {
    return glossary.entries().stream()
      .map(Map.Entry::getValue)
      .collect(Collectors.toList());
  }
}
