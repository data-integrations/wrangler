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
package io.cdap.wrangler.service.schema;

import io.cdap.wrangler.utils.AvroSchemaGlossary;
import io.cdap.wrangler.utils.PackagedSchemaLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singelton class for accessing an {@link AvroSchemaGlossary}
 */
public class DataModelGlossary {
  private static final Logger LOG = LoggerFactory.getLogger(DataModelGlossary.class);
  private static final AvroSchemaGlossary GLOSSARY;

  static {
    PackagedSchemaLoader loader = new PackagedSchemaLoader(
      DataModelGlossary.class.getClassLoader(), "schemas", "manifest.json");
    GLOSSARY = new AvroSchemaGlossary(loader);
    if (!GLOSSARY.configure()) {
      LOG.error("unable to load system data model schemas");
    }
  }

  public static AvroSchemaGlossary getGlossary() {
    return GLOSSARY;
  }
}
