/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Wrangler project info
 */
public class ProjectInfo {

  private static final Logger LOG = LoggerFactory.getLogger(ProjectInfo.class);
  private static final String RESOURCE_NAME = "/.properties";
  private static final String PLUGIN_VERSION = "plugin.version";
  private static final Map<String, String> PROPERTIES;

  // Initialize project properties from .properties file.
  static {
    Properties props = new Properties();
    try (InputStream resourceStream = ProjectInfo.class.getResourceAsStream(RESOURCE_NAME)) {
      props.load(resourceStream);
    } catch (Exception e) {
      LOG.warn("Unable to load the project properties {} ", e.getMessage(), e);
    }

    Map<String, String> properties = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      properties.put(key, props.getProperty(key));
    }
    PROPERTIES = properties;
  }

  /**
   * @return the project properties.
   */
  public static Map<String, String> getProperties() {
    return PROPERTIES;
  }

  /**
   * @return the project version.
   */
  public static String getVersion() {
    return PROPERTIES.get(PLUGIN_VERSION);
  }
}
