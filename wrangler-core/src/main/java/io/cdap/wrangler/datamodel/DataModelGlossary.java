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
package io.cdap.wrangler.datamodel;

import io.cdap.wrangler.utils.AvroSchemaGlossary;

/**
 * Singleton class for accessing an {@link AvroSchemaGlossary}
 */
public class DataModelGlossary {

  private static AvroSchemaGlossary glossary;

  /**
   * Initializes the data model glossary with the data models located at the url. All available
   * data models should be listed within a manifest.json file found at the root of the url.
   * The manifest.json should have the schema of {@link io.cdap.wrangler.utils.Manifest}. If
   * the loader is unable to download the manifest or referenced data models, the
   * initialization will fail. Below is an example manifest file.
   * e.g.
   *    {
   *      "standards": {
   *        "OMOP_6_0_0": {
   *          "format": "avsc"
   *        }
   *      }
   *    }
   * @param dataModelUrl the url to download the data models from.
   * @return true if successfully downloaded and store data models into glossary, otherwise
   *          false for all other errors.
   */
  public static boolean initialize(String dataModelUrl) {
    HTTPSchemaLoader schemaLoader = new HTTPSchemaLoader(dataModelUrl, "manifest.json");
    if (glossary == null) {
      glossary = new AvroSchemaGlossary(schemaLoader);
    } else {
      glossary.setAvroSchemaLoader(schemaLoader);
    }
    return glossary.configure();
  }

  /**
   * Accessor for the data model glossary.
   * @return the {@link AvroSchemaGlossary} instance.
   */
  public static AvroSchemaGlossary getGlossary() {
    return glossary;
  }
}
