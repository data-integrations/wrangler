/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link RecipeCompiler}
 */
public class RecipeCompilerTest {

  @Test
  public void testSuccessCompilation() throws Exception {
    try {
      Compiler compiler = new RecipeCompiler();
      CompiledUnit units = compiler.compile(
          "parse-as-csv :body ' ' true;"
        + "set-column :abc, :edf;"
        + "send-to-error exp:{ window < 10 } ;"
        + "parse-as-simple-date :col 'yyyy-mm-dd' :col 'test' :col2,:col4,:col9 10 exp:{test < 10};"
      );

      Assert.assertNotNull(units);
      Assert.assertEquals(4, units.size());
    } catch (CompileException e) {
      Assert.assertTrue(false);
    }
  }
}