/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.wrangler.metrics;

/**
 * Constants for emitting CDAP metrics from Wrangler
 */
public class Constants {
  public static final String APPLICATION_NAME = "dataprep";

  /**
   * Metric tags (same as those defined in CDAP)
   */
  public static final class Tags {
    public static final String APP_ENTITY_TYPE = "aet";
    public static final String APP_ENTITY_TYPE_NAME = "tpe";
    public static final String SERVICE = "srv";

  }
  private Constants() {
    throw new AssertionError("Cannot instantiate a static utility class.");
  }
}
