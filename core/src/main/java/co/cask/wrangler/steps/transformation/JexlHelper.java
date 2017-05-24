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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.common.Bytes;
import co.cask.wrangler.steps.transformation.functions.Conversions;
import co.cask.wrangler.steps.transformation.functions.DDL;
import co.cask.wrangler.steps.transformation.functions.DataQuality;
import co.cask.wrangler.steps.transformation.functions.Dates;
import co.cask.wrangler.steps.transformation.functions.GeoFences;
import co.cask.wrangler.steps.transformation.functions.JSON;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Collection of registered functions for jexl context.
 */
public final class JexlHelper {

  /**
   * @return configured {@link JexlEngine}
   */
  public static JexlEngine getEngine() {
    return new JexlBuilder()
      .namespaces(getRegisteredFunctions())
      .silent(false)
      .cache(10)
      .strict(true)
      .create();
  }

  /**
   * @return List of registered functions.
   */
  public static Map<String, Object> getRegisteredFunctions() {
    Map<String, Object> functions = new HashMap<>();
    functions.put(null, Conversions.class);
    functions.put("date", Dates.class);
    functions.put("json", JSON.class);
    functions.put("math", Math.class);
    functions.put("string", StringUtils.class);
    functions.put("bytes", Bytes.class);
    functions.put("arrays", Arrays.class);
    functions.put("dq", DataQuality.class);
    functions.put("ddl", DDL.class);
    functions.put("geo", GeoFences.class);
    return functions;
  }
}
