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

package co.cask.wrangler.statistics;

import io.dataapps.chlorine.finder.FinderEngine;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Created by kewang on 5/23/17.
 */
@Ignore
public class ChlorineFinderTest {

  private final FinderEngine engine;

  public ChlorineFinderTest() {
    engine = new FinderEngine("wrangler-finder.xml", true, false);
  }

  @Test
  public void testFinder() {
    String value = "478 Macpherson Drive";
    Map<String, List<String>> finds = engine.findWithType(value);
    StringBuilder result = new StringBuilder();
    for (String key : finds.keySet()) {
      List<String> types = finds.get(key);
      result.append(key);
      result.append("-->");
      for (String type : types) {
        result.append(type);
        result.append(", ");
      }
      result.append("\n");
    }
  }
}
