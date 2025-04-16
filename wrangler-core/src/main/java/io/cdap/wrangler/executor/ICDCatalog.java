/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.executor;


import io.cdap.directives.lookup.StaticCatalog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for loading and managing ICD codes.
 *
 * <p>
 *   icd10cm_code_2016.txt contains all ICD-10-CM (diagnosis) codes valid for FY2016.
 *   icd9cm_code_2015.txt contains ICD-9-CM (diagnosis) codes valid till FY2015
 * </p>
 */
public final class ICDCatalog implements StaticCatalog {

  // Type of ICD code 9 or 10 {2016,2017}.
  private final String name;

  // Map to store mapping from code to description.
  private Map<String, ICDCode> lookupTable = new HashMap<>();

  /**
   * Single ICD entry
   */
  public class ICDCode implements StaticCatalog.Entry {
    // Description associated with the code.
    String description;

    ICDCode(String description) {
      this.description = description;
    }

    /**
     * @return description for the code.
     */
    @Override
    public String getDescription() {
      return description;
    }
  }

  public ICDCatalog(String name) {
    this.name = name;
  }

  /**
   * Configures the ICD StaticCatalog by loading the appropriate stream.
   *
   * @return true if successfully configured, else false.
   */
  @Override
  public boolean configure() {
    String filename = String.format("%s_cm_codes.txt", name);
    InputStream in = ICDCatalog.class.getClassLoader().getResourceAsStream(filename);
    if (in == null) {
      return false;
    }
    InputStreamReader isr = new InputStreamReader(in);
    try (BufferedReader reader = new BufferedReader(isr)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String code = line.substring(0, 7).trim();
        String description = line.substring(8).trim();
        lookupTable.put(code, new ICDCode(description));
      }
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Looks up a ICD Code in the catalog and the associated {@link ICDCode} value.
   *
   * @param code to be looked up.
   * @return ICDCode if found, else null.
   */
  @Override
  public StaticCatalog.Entry lookup(String code)  {
    return lookupTable.get(code);
  }

  /**
   * @return name of the catalog.
   */
  @Override
  public String getCatalog() {
    return name;
  }
}
