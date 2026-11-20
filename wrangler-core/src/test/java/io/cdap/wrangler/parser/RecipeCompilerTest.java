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
import io.cdap.wrangler.api.CompileException;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.RecipeSymbol;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

/**
 * Tests {@link RecipeCompiler}
 */
public class RecipeCompilerTest {

  private static final Compiler compiler = new RecipeCompiler();

  @Test
  public void testSuccessCompilation() throws Exception {
    try {
      Compiler compiler = new RecipeCompiler();
      CompileStatus status = compiler.compile(
          "parse-as-csv :body ' ' true;\n"
        + "set-column :abc, :edf;\n"
        + "send-to-error exp:{ window < 10 } ;\n"
        + "parse-as-simple-date :col 'yyyy-mm-dd' :col 'test' :col2,:col4,:col9 10 exp:{test < 10};\n"
      );

      Assert.assertNotNull(status.getSymbols());
      Assert.assertEquals(4, status.getSymbols().size());
    } catch (CompileException e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testMacroSkippingDuringParsing() throws Exception {
    String[] recipe = new String[] {
      "parse-as-csv :body ',' true;",
      "${macro1}",
      "${macro${number}}",
      "parse-as-csv :body '${delimiter}' true;"
    };

    CompileStatus status = TestingRig.compile(recipe);
    Assert.assertEquals(true, status.isSuccess());
  }

  @Test
  public void testSingleMacroLikeWranglerPlugin() throws Exception {
    String[] recipe = new String[] {
      "${directives}"
    };

    CompileStatus status = TestingRig.compile(recipe);
    Assert.assertEquals(true, status.isSuccess());
  }

  @Test
  public void testSparedPragmaLoadDirectives() throws Exception {
    String[] recipe = new String[] {
      "#pragma load-directives test1,test2,test3,test4,test5;",
      "${directives}",
      "#pragma load-directives root1,root2,root3;"
    };
    TestingRig.compileSuccess(recipe);
  }

  @Test
  public void testNestedMacros() throws Exception {
    String[] recipe = new String[] {
      "#pragma load-directives test1,test2,test3,test4,test5;",
      "${directives_${number}}"
    };
    TestingRig.compileSuccess(recipe);
  }

  @Test
  public void testSemiColonMissing() throws Exception {
    String[] recipe = new String[] {
      "#pragma load-directives test1,test2,test3,test4,test5",
      "${directives_${number}}"
    };
    TestingRig.compileFailure(recipe);
  }

  @Test
  public void testMissingOpenBraceOnMacro() throws Exception {
    String[] recipe = new String[] {
      "#pragma load-directives test1,test2,test3,test4,test5;",
      "$directives}"
    };
    TestingRig.compileFailure(recipe);
  }

  @Test
  public void testMissingCloseBraceOnMacro() throws Exception {
    String[] recipe = new String[] {
      "#pragma load-directives test1,test2,test3,test4,test5;",
      "${directives"
    };
    TestingRig.compileFailure(recipe);
  }

  @Test
  public void testMissingBothBraceOnMacro() throws Exception {
    String[] recipe = new String[] {
      "#pragma load-directives test1,test2,test3,test4,test5;",
      "${directives"
    };
    TestingRig.compileFailure(recipe);
  }

  @Test
  public void testMissingPragmaHash() throws Exception {
    String[] recipe = new String[] {
      "pragma load-directives test1,test2,test3,test4,test5;",
    };
    TestingRig.compileFailure(recipe);
  }

  @Test
  public void testTypograhicalErrorPragmaLoadDirectives() throws Exception {
    String[] recipe = new String[] {
      "pragma test1,test2,test3,test4,test5;",
    };
    TestingRig.compileFailure(recipe);
  }

  @Test
  public void testWithIfStatement() throws Exception {
    String[] recipe = new String[] {
      "#pragma load-directives test1,test2;",
      "${macro_1}",
      "if ((test > 10) && (window < 20)) {  parse-as-csv :body ',' true; if (window > 10) " +
        "{ send-to-error exp:{test > 10}; } }"
    };
    TestingRig.compileSuccess(recipe);
  }

  @Test
  public void testComplexExpression() throws Exception {
    String[] recipe = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "merge body_1 body_2 Full_Name ' '",
      "drop body_1,body_2",
      "find-and-replace body_4 s/Washington//g",
      "send-to-error empty(body_4)",
      "send-to-error body_5 =~ \"DC.*\"",
      "filter-rows-on regex-match body_5 *as*"
    };
    CompileStatus compile = TestingRig.compile(recipe);
    Assert.assertTrue(true);
  }

  @Test
  public void test() throws Exception {
    String[] recipe = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "merge body_1 body_2 Full_Name ' '",
      "drop body_1,body_2",
      "find-and-replace body_4 s/Washington//g",
      "send-to-error empty(body_4)",
      "send-to-error body_5 =~ \"DC.*\"",
      "filter-rows-on regex-match body_5 *as*"
    };
    CompileStatus compile = TestingRig.compile(recipe);
    Assert.assertTrue(true);
  }

  @Test
  public void testSingleLineDirectives() throws Exception {
    String[] recipe = new String[] {
      "parse-as-csv :body '\t' true; drop :body;"
    };
    CompileStatus compile = TestingRig.compile(recipe);
    Assert.assertTrue(true);
  }

  @Test
  public void testError() throws Exception {
    String[] recipe = new String[] {
      "parse-as-abababa-csv :body '\t' true; drop :body;"
    };
    CompileStatus compile = TestingRig.compile(recipe);
    Assert.assertTrue(true);
  }

  @Test
  public void testRecipePragmaWithCompiler() throws Exception {
    String[] recipe = new String[] {
      "#pragma load-directives test1,test2,test3,test4;",
      "${directives}"
    };
    CompileStatus compile = TestingRig.compile(recipe);
    Set<String> loadableDirectives = compile.getSymbols().getLoadableDirectives();
    Assert.assertEquals(4, loadableDirectives.size());
  }

  @Test
  public void testParsingByteSizeAndTimeDuration() throws Exception {
    // This recipe uses a dummy directive assuming it accepts these types.
    // The goal is only to check if the compiler/parser can handle the syntax.
    String[] recipe = new String[] {
            "some-directive :col1 '10KB' '5s';",
            "some-directive :col2 '1.5MB' '2.2m';",
            "some-directive :col3 '500' '1h';", // Plain bytes, time
            "some-directive :col4 '1g' '10ns';", // Lowercase g, nanoseconds
    };
    CompileStatus status = TestingRig.compile(recipe);
    Assert.assertTrue("Recipe compilation failed for ByteSize/TimeDuration syntax", status.isSuccess());

    RecipeSymbol symbols = status.getSymbols();
    Assert.assertNotNull(symbols);
    Assert.assertEquals(4, symbols.size()); // Should parse 4 directives

    // Check the token types parsed for the first directive
    Iterator<TokenGroup> groupIterator = symbols.iterator();
    Assert.assertTrue(groupIterator.hasNext());
    TokenGroup firstGroup = groupIterator.next();
    Assert.assertNotNull(firstGroup);
    Assert.assertEquals(4, firstGroup.size()); // directive_name, column_name, byte_size, time_duration

    Iterator<Token> tokenIterator = firstGroup.iterator();

    Token directiveToken = tokenIterator.next();
    Assert.assertEquals(TokenType.DIRECTIVE_NAME, directiveToken.type());
    Assert.assertEquals("some-directive", directiveToken.value());

    Token columnToken = tokenIterator.next();
    Assert.assertEquals(TokenType.COLUMN_NAME, columnToken.type());
    Assert.assertEquals("col1", columnToken.value()); // Assuming ColumnName doesn't include :

    Token sizeToken = tokenIterator.next();
    Assert.assertTrue("Expected ByteSize token, got " + sizeToken.getClass().getSimpleName(),
                      sizeToken instanceof ByteSize);
    Assert.assertEquals(TokenType.BYTE_SIZE, sizeToken.type());
    Assert.assertEquals("10KB", sizeToken.value());

    Token timeToken = tokenIterator.next();
    Assert.assertTrue("Expected TimeDuration token, got " + timeToken.getClass().getSimpleName(),
                      timeToken instanceof TimeDuration);
    Assert.assertEquals(TokenType.TIME_DURATION, timeToken.type());
    Assert.assertEquals("5s", timeToken.value());

    Assert.assertFalse(tokenIterator.hasNext()); // Ensure no extra tokens
  }

  @Test
  public void testParsingInvalidByteSizeAndTimeDuration() throws Exception {
    String[] recipe1 = new String[] {"some-directive :col '10XB';"}; // Invalid unit
    String[] recipe2 = new String[] {"some-directive :col '--10s';"}; // Invalid number
    String[] recipe3 = new String[] {"some-directive :col '10 KB MB';"}; // Multiple units

    TestingRig.compileFailure(recipe1);
    TestingRig.compileFailure(recipe2);
    TestingRig.compileFailure(recipe3);
  }
}
