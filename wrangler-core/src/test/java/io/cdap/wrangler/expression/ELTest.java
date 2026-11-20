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

package io.cdap.wrangler.expression;

import io.cdap.wrangler.api.DefaultJexlAllowlist;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.JexlConfiguration;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link EL}
 */
public class ELTest {

  @Test
  public void testBasicFunctionality() throws Exception {
    EL el = EL.compile("a + b");
    ELResult execute = el.execute(new ELContext().add("a", 1).add("b", 2));
    Assert.assertNotNull(execute);
    Assert.assertEquals(new Integer(3), execute.getInteger());
    Assert.assertTrue(el.variables().contains("a"));
    Assert.assertFalse(el.variables().contains("c"));
  }

  @Test(expected = ELException.class)
  public void testUndefinedVariableException() throws Exception {
    EL el = EL.compile("a + b + c");
    el.execute(new ELContext().add("a", 1).add("b", 2));
  }

  @Test
  public void testArrays() throws Exception {
    EL el = EL.compile("runtime['map'] > token['ABC.EDFG']['input'] " +
                         "&& math:max(toDouble(runtime['map']), toDouble(token['ABC.EDFG']['input'])) > 9");

    Map<String, Object> runtime = new HashMap<>();
    runtime.put("map", "10");
    ELContext ctx = new ELContext();
    ctx.add("runtime", runtime);

    Map<String, Map<String, Object>> token = new HashMap<>();
    Map<String, Object> stage1 = new HashMap<>();
    stage1.put("input", "1");
    token.put("ABC.EDFG", stage1);
    ctx.add("token", token);
    ELResult execute = el.execute(ctx);
    Assert.assertEquals(true, execute.getBoolean());
  }

  @Test
  public void testDecimalTransform() throws Exception {
    BigDecimal bd = new BigDecimal("123456789.123456789");
    int n = 2;

    List<String> directives = new ArrayList<>();
    directives.add("precision");
    directives.add("abs");
    directives.add("scale");
    directives.add("unscaled");
    directives.add("negate");
    directives.add("strip_zero");
    directives.add("sign");

    List<String> directives2 = new ArrayList<>();
    directives2.add("pow");
    directives2.add("decimal_left");
    directives2.add("decimal_right");
    directives2.add("add");
    directives2.add("subtract");
    directives2.add("multiply");
    directives2.add("divideq");
    directives2.add("divider");

    for (String directive : directives) {
      EL el = EL.compile(String.format("decimal:%s(a)", directive));
      ELResult execute = el.execute(new ELContext().add("a", bd));

      Assert.assertNotNull(execute);

      switch (directive) {
        case "abs":
          Assert.assertEquals(bd.abs(), execute.getObject());
          break;
        case "scale":
          Assert.assertEquals(bd.scale(), execute.getObject());
          break;
        case "unscaled":
          Assert.assertEquals(bd.unscaledValue(), execute.getObject());
          break;
        case "precision":
          Assert.assertEquals(bd.precision(), execute.getObject());
          break;
        case "negate":
          Assert.assertEquals(bd.negate(), execute.getObject());
          break;
        case "strip_zero":
          Assert.assertEquals(bd.stripTrailingZeros(), execute.getObject());
          break;
        case "sign":
          Assert.assertEquals(bd.signum(), execute.getObject());
          break;
      }
    }

    for (String directive : directives2) {
      EL el = EL.compile(String.format("decimal:%s(a, b)", directive));
      ELResult execute = el.execute(new ELContext().add("a", bd).add("b", n));

      Assert.assertNotNull(execute);

      switch (directive) {
        case "pow":
          Assert.assertEquals(bd.pow(n), execute.getObject());
          break;
        case "decimal_left":
          Assert.assertEquals(new BigDecimal("1234567.89123456789"), execute.getObject());
          break;
        case "decimal_right":
          Assert.assertEquals(new BigDecimal("12345678912.3456789"), execute.getObject());
          break;
        case "add":
          Assert.assertEquals(bd.add(BigDecimal.valueOf(n)), execute.getObject());
          break;
        case "subtract":
          Assert.assertEquals(bd.subtract(BigDecimal.valueOf(n)), execute.getObject());
          break;
        case "multiply":
          Assert.assertEquals(bd.multiply(BigDecimal.valueOf(n)), execute.getObject());
          break;
        case "divideq":
          Assert.assertEquals(bd.divide(BigDecimal.valueOf(n), BigDecimal.ROUND_HALF_EVEN), execute.getObject());
          break;
        case "divider":
          Assert.assertEquals(bd.remainder(BigDecimal.valueOf(n)), execute.getObject());
          break;
      }
    }
  }

  @Test
  public void testArithmeticOperations() throws Exception {
    BigDecimal bd1 = new BigDecimal("123.123");
    BigDecimal bd2 = new BigDecimal("456.456");
    Integer i1 = 123;
    Integer i2 = 456;
    Double d1 = 123.123d;
    Double d2 = 456.456d;
    Float f1 = 123.123f;
    Float f2 = 456.456f;

    EL el = EL.compile("arithmetic:add(a,b)");
    ELResult execute = el.execute(new ELContext().add("a", bd1).add("b", bd2));
    Assert.assertEquals(bd1.add(bd2), execute.getObject());
    el = EL.compile("arithmetic:minus(a,b)");
    execute = el.execute(new ELContext().add("a", f1).add("b", f2));
    Assert.assertEquals(f1 - f2, execute.getObject());
    el = EL.compile("arithmetic:multiply(a,b)");
    execute = el.execute(new ELContext().add("a", d1).add("b", d2));
    Assert.assertEquals(d1 * d2, execute.getObject());
    el = EL.compile("arithmetic:divideq(a,b)");
    execute = el.execute(new ELContext().add("a", bd1).add("b", bd2));
    Assert.assertEquals(bd1.divide(bd2, BigDecimal.ROUND_HALF_EVEN).stripTrailingZeros(), execute.getObject());
    el = EL.compile("arithmetic:divider(a,b)");
    execute = el.execute(new ELContext().add("a", i1).add("b", i2));
    Assert.assertEquals(i1 % i2, execute.getObject());
    el = EL.compile("arithmetic:lcm(a,b)");
    execute = el.execute(new ELContext().add("a", bd1).add("b", bd2));
    Assert.assertEquals(new BigDecimal("18714.696"), execute.getObject());
    el = EL.compile("arithmetic:equal(a,b)");
    execute = el.execute(new ELContext().add("a", i1).add("b", i2));
    Assert.assertEquals(false, execute.getObject());
    el = EL.compile("arithmetic:max(a,b)");
    execute = el.execute(new ELContext().add("a", f1).add("b", f2));
    Assert.assertEquals(f2, execute.getObject());
    el = EL.compile("arithmetic:min(a,b)");
    execute = el.execute(new ELContext().add("a", d1).add("b", d2));
    Assert.assertEquals(d1, execute.getObject());
  }

  @Test
  public void testDefaultCompileOptionsEnablesAllowlistAndBlocksReflection() {
    Assert.assertTrue(CompileOptions.getDefaultCompileOptions().isAllowlistEnabled());
    Assert.assertNotNull(CompileOptions.getDefaultCompileOptions().getJexlAllowlist());
    Assert.assertFalse(CompileOptions.getDefaultCompileOptions().getJexlAllowlist().isEmpty());

    List<String> disallowedExpressions = Arrays.asList(
        "''.getClass()",
        "s.getClass()",
        "''.getClass().forName('java.lang.Runtime')",
        "''.class",
        "s.class",
        "s['class']",
        "{ : }.class",
        "{'a': 1}.class",
        "{ : }.class.forName('java.lang.Runtime')",
        "(1).TYPE",
        "true.TYPE",
        "(1)['TYPE']",
        "s.wait()",
        "s.notify()",
        "s.notifyAll()",
        "s.class = null",
        "s['class'] = null",
        "math:forName('java.lang.Runtime')",
        "math:getClassLoader()",
        "string:forName('java.lang.ProcessBuilder')",
        "date:newInstance()",
        "bytes:forName('java.lang.Runtime')",
        "arrays:toString()",
        "bytes:toString()",
        "new('java.lang.ProcessBuilder', 'id')",
        "new('java.io.File', '/etc/passwd')",
        "new('java.net.URL', 'http://example.com')",
        "new('javax.script.ScriptEngineManager')",
        "new('java.lang.String', 'abc')"
    );

    for (String expr : disallowedExpressions) {
      try {
        EL el = EL.compile(expr);
        el.execute(new ELContext().add("s", "hello"));
        Assert.fail("Expected ELPermissionException for disallowed expression: " + expr);
      } catch (ELPermissionException expected) {
        // expected
      } catch (ELException e) {
        Assert.fail("Expected ELPermissionException but got ELException for: " + expr + " (" + e.getMessage() + ")");
      }
    }
  }

  @Test
  public void testBlockedDisallowedContextObjects() throws Exception {
    Map<String, String> blockedCalls = new HashMap<>();
    blockedCalls.put("rt.exec('id')", "Runtime.exec");
    blockedCalls.put("th.getContextClassLoader()", "Thread.getContextClassLoader");
    blockedCalls.put("pb.start()", "ProcessBuilder.start");
    blockedCalls.put("cls.forName('java.lang.Runtime')", "Class.forName on Class.class");
    blockedCalls.put("strCls.forName('java.lang.Runtime')", "Class.forName on String.class");
    blockedCalls.put("strCls.classLoader", "Property get on Class<?>");
    blockedCalls.put("strCls['classLoader'] = null", "Property set on Class<?>");
    blockedCalls.put("objCls.getMethods()", "Class.getMethods on Object.class");

    ELContext context = new ELContext()
        .add("rt", Runtime.getRuntime())
        .add("th", Thread.currentThread())
        .add("pb", new ProcessBuilder("id"))
        .add("cls", Class.class)
        .add("strCls", String.class)
        .add("objCls", Object.class);

    for (Map.Entry<String, String> entry : blockedCalls.entrySet()) {
      EL el = EL.compile(entry.getKey());
      try {
        el.execute(context);
        Assert.fail("Expected ELPermissionException for " + entry.getValue());
      } catch (ELPermissionException expected) {
        // expected
      }
    }
  }

  @Test
  public void testCompileOptionsBranchesAndAllowlistedWranglerTypes() throws Exception {
    DirectiveConfig emptyListConfig =
        new DirectiveConfig(null, null, new JexlConfiguration(true, Collections.emptyList()));
    CompileOptions emptyOptions =
        CompileOptions.fromContext(new ConfigDirectiveContext(emptyListConfig, true));
    Assert.assertTrue(emptyOptions.isAllowlistEnabled());
    Assert.assertTrue(emptyOptions.getJexlAllowlist().isEmpty());

    DirectiveConfig nullListEnabledConfig =
        new DirectiveConfig(null, null, new JexlConfiguration(true, null));
    CompileOptions defaultFallbackOptions =
        CompileOptions.fromContext(new ConfigDirectiveContext(nullListEnabledConfig, true));
    Assert.assertTrue(defaultFallbackOptions.isAllowlistEnabled());
    Assert.assertEquals(DefaultJexlAllowlist.get(), defaultFallbackOptions.getJexlAllowlist());

    CompileOptions disabledOptions =
        CompileOptions.fromContext(new ConfigDirectiveContext(nullListEnabledConfig, false));
    Assert.assertFalse(disabledOptions.isAllowlistEnabled());
    Assert.assertEquals(DefaultJexlAllowlist.get(), disabledOptions.getJexlAllowlist());

    Row row = new Row("col", "val");
    ELContext.Context ctx = new ELContext.Context("TRANSFORM", "stage");
    EL el = EL.compile(
        "bytes:toString(bytes:toBytes('ok')) == 'ok' && arrays:asList('a', 'b').contains('a') "
            + "&& datetime:CurrentTimestamp().getZone().getId() != null "
            + "&& this.width() == 1 && this.getFields().get(0).getFirst() == 'col' && ctx.name == 'stage'");
    ELResult result = el.execute(new ELContext().add("this", row).add("ctx", ctx));
    Assert.assertEquals(Boolean.TRUE, result.getBoolean());
  }
}
