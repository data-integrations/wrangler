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

import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.JexlConfiguration;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELPermissionException;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link Precondition}
 */
public class PreconditionTest {

  @Test
  public void testPrecondition() throws Exception {
    Row row = new Row("a", 1).add("b", "x").add("c", 2.06);
    Assert.assertEquals(true, new Precondition("a == 1 && b == \"x\"").apply(row));
    Assert.assertEquals(true, new Precondition("c > 2.0").apply(row));
    Assert.assertEquals(true, new Precondition("true").apply(row));
    Assert.assertEquals(false, new Precondition("false").apply(row));
    Assert.assertEquals(false, new Precondition(null).apply(row));
    Assert.assertEquals(false, new Precondition("").apply(row));
    Assert.assertEquals(false, new Precondition("   ").apply(row));
  }

  @Test
  public void testLegitimateStringMathAndDateExpressions() throws Exception {
    Row row = new Row("name", "Alice_Smith")
        .add("score", -15)
        .add("dateStr", "2026-01-15")
        .add("nullableCol", null);
    Assert.assertTrue(new Precondition("name.startsWith('Alice') && name.toLowerCase().contains('smith')").apply(row));
    Assert.assertTrue(new Precondition("string:isNotEmpty(name) && math:abs(score) == 15").apply(row));
    Assert.assertTrue(new Precondition("datetime:YearFromDate(datetime:GetDate(dateStr)) == 2026").apply(row));
    Assert.assertTrue(new Precondition("IsNull(nullableCol) && IsNotNull(name)").apply(row));
    Assert.assertTrue(new Precondition("this.width() == 4").apply(row));
    Row rowWithThisCol = new Row("this", "shadow").add("a", 1);
    Assert.assertTrue(new Precondition("this.width() == 2 && a == 1").apply(rowWithThisCol));
  }

  @Test
  public void testBadCondition() {
    Row row = new Row("a", 1).add("b", "x").add("c", 2.06);
    try {
      new Precondition("c").apply(row);
      Assert.fail("Expected PreconditionException for non-boolean expression");
    } catch (PreconditionException e) {
      Assert.assertEquals("Precondition 'c' does not result in true or false.", e.getMessage());
      Assert.assertNull(e.getCause());
      Assert.assertFalse(WranglerErrorUtil.isCriticalException(e));
    }
  }

  @Test
  public void testInvalidSyntaxAndRuntimeELExceptionAreNonCritical() throws Exception {
    Row row = new Row("a", 1).add("zero", 0);
    try {
      new Precondition("a &&");
      Assert.fail("Expected PreconditionException for invalid JEXL syntax");
    } catch (PreconditionException e) {
      Assert.assertTrue(e.getCause() instanceof ELException);
      Assert.assertFalse(WranglerErrorUtil.isCriticalException(e));
    }

    Precondition divideByZero = new Precondition("a / zero == 1");
    try {
      divideByZero.apply(row);
      Assert.fail("Expected PreconditionException for arithmetic error");
    } catch (PreconditionException e) {
      Assert.assertTrue(e.getCause() instanceof ELException);
      Assert.assertFalse(WranglerErrorUtil.isCriticalException(e));
    }
  }

  @Test
  public void testBlockedReflectionAndExecutionGadgets() {
    Row row = new Row("a", 1).add("b", "hello");
    List<String> disallowedExpressions = Arrays.asList(
        "''.getClass() != null",
        "b.getClass() != null",
        "''.getClass().forName('java.lang.Runtime') != null",
        "''.class != null",
        "b.class != null",
        "b['class'] != null",
        "{ : }.class != null",
        "{'a': 1}.class != null",
        "{ : }.class.forName('java.lang.Runtime') != null",
        "(1).TYPE != null",
        "math:forName('java.lang.Runtime') != null",
        "math:getClassLoader() != null",
        "string:forName('java.lang.ProcessBuilder') != null",
        "new('java.lang.ProcessBuilder', 'id') != null",
        "new('java.io.File', '/etc/passwd') != null",
        "new('java.net.URL', 'http://example.com') != null",
        "new('javax.script.ScriptEngineManager') != null",
        "new('java.lang.String', 'abc') == 'abc'"
    );

    for (String expr : disallowedExpressions) {
      try {
        Precondition precondition = new Precondition(expr);
        precondition.apply(row);
        Assert.fail("Expected PreconditionException for disallowed expression: " + expr);
      } catch (PreconditionException e) {
        Assert.assertTrue(
            "Expected ELPermissionException in causal chain for: " + expr,
            WranglerErrorUtil.isCriticalException(e));
        Assert.assertTrue(e.getCause() instanceof ELPermissionException);
      }
    }
  }

  @Test
  public void testPreconditionWithDirectiveContext() throws Exception {
    Row row = new Row("a", 1).add("b", "hello");
    DirectiveConfig emptyAllowlistConfig = new DirectiveConfig(
        null, null, new JexlConfiguration(true, Collections.emptyList()));
    ConfigDirectiveContext strictContext = new ConfigDirectiveContext(emptyAllowlistConfig, true);

    Precondition blockedStringCall = new Precondition("b.startsWith('h')", strictContext);
    try {
      blockedStringCall.apply(row);
      Assert.fail("Expected PreconditionException when allowlist is empty");
    } catch (PreconditionException e) {
      Assert.assertTrue(WranglerErrorUtil.isCriticalException(e));
    }

    Assert.assertTrue(new Precondition("b.startsWith('h')").apply(row));

    DirectiveConfig defaultAllowlistConfig = new DirectiveConfig(
        null, null, new JexlConfiguration(true, null));
    ConfigDirectiveContext defaultContext = new ConfigDirectiveContext(defaultAllowlistConfig, true);
    Precondition allowedStringCall = new Precondition("b.startsWith('h')", defaultContext);
    Assert.assertTrue(allowedStringCall.apply(row));
  }
}
