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

import java.io.IOException;


/**
 * The {@link AvroSchemaLoader} interface abstracts loading an AVRO data model schema definitions.
 */
public interface AvroSchemaLoader {

  /**
   * Loads and parses an AVRO data model schema.
   *
   * @return a map with keys representing the name of the schema. The value is a set of all of the revisions of the
   * {@link Schema}.
   */
  SetValuedMap<String, Schema> load() throws IOException;
}
