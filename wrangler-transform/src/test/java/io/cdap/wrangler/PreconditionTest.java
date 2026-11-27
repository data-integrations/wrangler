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

import io.cdap.wrangler.api.DefaultJexlAllowlist;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.JexlAllowlist;
import io.cdap.wrangler.api.JexlConfiguration;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.expression.ELPermissionException;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import org.apache.commons.jexl3.JexlException;
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
  }

  @Test(expected = PreconditionException.class)
  public void testBadCondition() throws Exception {
    Row row = new Row("a", 1).add("b", "x").add("c", 2.06);
    Assert.assertEquals(true, new Precondition("c").apply(row));
  }

  @Test
  public void testAllowlistedExpressionsWithSandboxEnabled() throws Exception {
    DirectiveContext context = createSandboxContext();
    Row row = new Row("a", 1).add("b", "hello").add("c", null);
    Assert.assertTrue(new Precondition("b.startsWith(\"he\") && a == 1", context).apply(row));
    Assert.assertFalse(new Precondition("b.startsWith(\"world\")", context).apply(row));
    Assert.assertTrue(new Precondition("c == null", context).apply(row));
  }

  @Test
  public void testDisallowedMethodBlockedWithSandboxEnabled() throws Exception {
    DirectiveContext context = createSandboxContext();
    Row row = new Row("body", "test");
    Precondition precondition = new Precondition(
        "body.getClass().forName(\"java.lang.Runtime\").getMethod(\"getRuntime\").invoke(null) != null",
        context);
    assertPermissionDenied(precondition, row, JexlException.Method.class);
  }

  @Test
  public void testDisallowedConstructorBlockedWithSandboxEnabled() throws Exception {
    DirectiveContext context = createSandboxContext();
    Row row = new Row("body", "test");
    Precondition precondition = new Precondition(
        "new('java.net.URL', 'http://169.254.169.254') != null",
        context);
    assertPermissionDenied(precondition, row, JexlException.Method.class);
  }

  @Test
  public void testDisallowedPropertyBlockedWithSandboxEnabled() throws Exception {
    JexlAllowlist rule = new JexlAllowlist(
        SampleBean.class.getName(),
        Collections.emptyList(),
        Collections.singletonList("allowed"));
    DirectiveContext context = createSandboxContext(Collections.singletonList(rule));

    Row row = new Row("bean", new SampleBean());
    Assert.assertTrue(new Precondition("bean.allowed == 'ok'", context).apply(row));
    assertPermissionDenied(new Precondition("bean.secret == 'secret'", context), row, JexlException.Property.class);
  }

  @Test
  public void testSandboxDisabledAllowsNonAllowlistedMethods() throws Exception {
    DirectiveContext context = createUnsandboxedContext();
    Row row = new Row("body", "test");
    Precondition precondition = new Precondition(
        "new('java.lang.Object').getClass().getName() == 'java.lang.Object'", context);
    Assert.assertTrue(precondition.apply(row));
  }

  @Test
  public void testSandboxDisabledUnresolvableMethodThrowsPreconditionException() throws Exception {
    DirectiveContext context = createUnsandboxedContext();
    Row row = new Row("body", "test");
    try {
      new Precondition("body.nonExistentMethod() == true", context).apply(row);
      Assert.fail("Expected PreconditionException when allowlist is disabled");
    } catch (PreconditionException e) {
      Assert.assertTrue(e.getMessage().contains("nonExistentMethod"));
    }
  }

  @Test
  public void testEmptyAllowlistBlocksAllMethods() throws Exception {
    DirectiveContext context = createSandboxContext(Collections.emptyList());
    Row row = new Row("b", "hello");
    assertPermissionDenied(new Precondition("b.length() == 5", context), row, JexlException.Method.class);
  }

  @Test
  public void testSyntaxAndEvaluationErrorsThrowPreconditionException() throws Exception {
    DirectiveContext context = createSandboxContext();
    Row row = new Row("b", "hi");

    try {
      new Precondition("a ===", context);
      Assert.fail("Expected PreconditionException for syntax error");
    } catch (PreconditionException e) {
      Assert.assertNotNull(e.getMessage());
    }

    try {
      new Precondition("a == 99999999999999999999L", context);
      Assert.fail("Expected PreconditionException for out-of-range numeric literal");
    } catch (PreconditionException e) {
      Assert.assertTrue(e.getMessage().contains("99999999999999999999"));
    }

    try {
      new Precondition("missing == 1", context).apply(row);
      Assert.fail("Expected PreconditionException for undefined variable");
    } catch (PreconditionException e) {
      Assert.assertTrue(e.getMessage().contains("missing"));
    }

    try {
      new Precondition("b > 1", context).apply(row);
      Assert.fail("Expected PreconditionException for runtime type conversion error");
    } catch (PreconditionException e) {
      Assert.assertTrue(e.getMessage().contains("hi"));
    }
  }

  @Test
  public void testWrappedExceptionCauseMessageFallback() throws Exception {
    JexlAllowlist rule = new JexlAllowlist(
        SampleBean.class.getName(),
        Arrays.asList("failWithMessage", "failWithoutMessage"),
        Collections.emptyList());
    DirectiveContext context = createSandboxContext(Collections.singletonList(rule));
    Row row = new Row("bean", new SampleBean());

    try {
      new Precondition("bean.failWithMessage() == 'ok'", context).apply(row);
      Assert.fail("Expected PreconditionException");
    } catch (PreconditionException e) {
      Assert.assertEquals("boom", e.getMessage());
    }

    try {
      new Precondition("bean.failWithoutMessage() == 'ok'", context).apply(row);
      Assert.fail("Expected PreconditionException");
    } catch (PreconditionException e) {
      Assert.assertNotNull(e.getMessage());
      Assert.assertTrue(e.getMessage().contains("failWithoutMessage"));
    }
  }

  private static DirectiveContext createSandboxContext() {
    return createSandboxContext(DefaultJexlAllowlist.get());
  }

  private static DirectiveContext createSandboxContext(List<JexlAllowlist> allowlist) {
    return createContext(true, allowlist);
  }

  private static DirectiveContext createUnsandboxedContext() {
    return createContext(false, DefaultJexlAllowlist.get());
  }

  private static DirectiveContext createContext(boolean allowlistEnabled, List<JexlAllowlist> allowlist) {
    JexlConfiguration jexlConfig = new JexlConfiguration(allowlistEnabled, allowlist);
    DirectiveConfig config = new DirectiveConfig(null, null, jexlConfig);
    return new ConfigDirectiveContext(config, allowlistEnabled);
  }

  private static void assertPermissionDenied(
      Precondition precondition, Row row, Class<? extends JexlException> expectedCause)
      throws PreconditionException {
    try {
      precondition.apply(row);
      Assert.fail("Expected ELPermissionException");
    } catch (ELPermissionException e) {
      Assert.assertTrue(
          "Expected cause " + expectedCause.getSimpleName() + ", got " + e.getCause(),
          expectedCause.isInstance(e.getCause()));
      Assert.assertTrue(
          e.getMessage().contains(
              "Make sure the JEXL transformation is valid and uses only allowlisted classes, "
                  + "methods, and properties."));
    }
  }

  /**
   * Minimal bean used to verify property-level allowlist enforcement and exception message handling.
   */
  public static final class SampleBean {
    public String getAllowed() {
      return "ok";
    }

    public String getSecret() {
      return "secret";
    }

    public String failWithMessage() {
      throw new IllegalStateException("boom");
    }

    public String failWithoutMessage() {
      throw new IllegalStateException();
    }
  }
}
