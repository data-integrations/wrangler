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

package io.cdap.wrangler;

import org.junit.Assert;
import org.junit.Test;

/**
 * Class description here.
 */
public class ExperimentTests {

  private String getIdFromName(String name) {
    name = name.toLowerCase();
    name = name.replaceAll("[_ \t]+", "-");
    name = name.replaceAll("[/$%#@**&()!,~+=?><|}{]+", "");
    return name;
  }

  @Test
  public void testIdCreationFromName() throws Exception {
    String[] names = {
      "My Sample Recipe",
      "SSGT Transformation Recipe!",
      "{SSGT Transformation Recipe!}",
      "{SSGT Transformation Recipe!}<sample-file>",
      "test>???>>>>window",
      "test    test1",
      "window\t    \t   window1"
    };

    String[] expected = {
      "my-sample-recipe",
      "ssgt-transformation-recipe",
      "ssgt-transformation-recipe",
      "ssgt-transformation-recipesample-file",
      "testwindow",
      "test-test1",
      "window-window1"
    };

    for (int i = 0; i < names.length; ++i) {
      String name = names[i];
      String expect = expected[i];
      String id = getIdFromName(name);
      Assert.assertEquals(expect, id);
    }
  }

}
