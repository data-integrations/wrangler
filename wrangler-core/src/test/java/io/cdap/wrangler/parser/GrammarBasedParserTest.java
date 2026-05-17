/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.RecipeSymbol;
import io.cdap.wrangler.api.Triplet;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.DirectiveName;
import io.cdap.wrangler.api.parser.Expression;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.Properties;
import io.cdap.wrangler.api.parser.Ranges;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link GrammarBasedParser}
 */
public class GrammarBasedParserTest {

  @Test
  public void testBasic() throws Exception {
    String[] recipe = new String[] {
      "#pragma version 2.0;",
      "rename :col1 :col2",
      "parse-as-csv :body ',' true;",
      "#pragma load-directives text-reverse, text-exchange;",
      "${macro} ${macro_2}",
      "${macro_${test}}"
    };

    RecipeParser parser = TestingRig.parse(recipe);
    List<Directive> directives = parser.parse();
    Assert.assertEquals(2, directives.size());
  }

  @Test
  public void testLoadableDirectives() throws Exception {
    String[] recipe = new String[] {
      "#pragma version 2.0;",
      "#pragma load-directives text-reverse, text-exchange;",
      "rename col1 col2",
      "parse-as-csv body , true",
      "text-reverse :body;",
      "test prop: { a='b', b=1.0, c=true};",
      "#pragma load-directives test-change,text-exchange, test1,test2,test3,test4;"
    };

    Compiler compiler = new RecipeCompiler();
    CompileStatus status = compiler.compile(new MigrateToV2(recipe).migrate());
    Assert.assertEquals(7, status.getSymbols().getLoadableDirectives().size());
  }

  @Test
  public void testCommentOnlyRecipe() throws Exception {
    String[] recipe = new String[] {
      "// test"
    };

    RecipeParser parser = TestingRig.parse(recipe);
    List<Directive> directives = parser.parse();
    Assert.assertEquals(0, directives.size());
  }

  @Test
  public void testByteSizeAndTimeDurationSyntax() throws Exception {
    // Test valid byte size syntax
    String[] recipe = new String[] {
      "aggregate-stats :file_size :duration total_size total_time size-unit:GB time-unit:hours"
    };
    CompileStatus status = TestingRig.compile(recipe);
    Assert.assertTrue(status.isSuccess());

    // Test valid time duration syntax
    recipe = new String[] {
      "set-timeout 5m"
    };
    status = TestingRig.compile(recipe);
    Assert.assertTrue(status.isSuccess());

    // Test invalid byte size syntax
    try {
      recipe = new String[] {
        "aggregate-stats :file_size :duration total_size total_time size-unit:invalid time-unit:hours"
      };
      TestingRig.compile(recipe);
      Assert.fail("Expected parse exception for invalid size unit");
    } catch (Exception e) {
      // Expected
    }

    // Test invalid time duration syntax
    try {
      recipe = new String[] {
        "set-timeout 5x"
      };
      TestingRig.compile(recipe);
      Assert.fail("Expected parse exception for invalid time unit");
    } catch (Exception e) {
      // Expected
    }
  }

}
