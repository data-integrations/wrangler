/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.lookup.internal;


import co.cask.wrangler.api.StaticCatalog;
import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.Reader;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

/**
 * Class for loading and managing Maxmind City IP Lookup.
 *
 * <p>
 *   This reads the MaxMind Open Source MMDB.
 * </p>
 */
public final class MaxMindGeoLookup implements StaticCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(MaxMindGeoLookup.class);

  // Type of MMDB to lookup from Country or City are the two types supported.
  private final String name;

  private Reader reader;

  public MaxMindGeoLookup(String name) {
    this.name = name;
  }

  /**
   * Configures the ICD StaticCatalog by loading the appropriate stream.
   *
   * @return true if successfully configured, else false.
   */
  @Override
  public boolean configure() {
    String filename = String.format("GeoLite2-%s.mmdb", StringUtils.capitalize(name));
    URL resource = MaxMindGeoLookup.class.getClassLoader().getResource(filename);
    File file = new File(resource.getFile());
    try {
      reader = new Reader(file);
    } catch (IOException e) {
      return false;
    }

    return true;
  }

  /**
   * Looks up a Geo Code in the Maxmind Geo Database.
   *
   * @param code to be looked up.
   * @return ICDCode if found, else null.
   */
  @Override
  public Entry lookup(String code)  {
    try {
      InetAddress address = InetAddress.getByName(code);
      final JsonNode response = reader.get(address);
      if (response != null) {
        return new Entry() {
          @Override
          public JSONObject get() {
            JSONObject object = (JSONObject) new JSONTokener(response.toString()).nextValue();
            return object;
          }
        };
      }
      return null;
    } catch (UnknownHostException e) {
      return null;
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * @return name of the catalog.
   */
  @Override
  public String getCatalog() {
    return name;
  }
}
