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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link EL}
 */
public class ELTest {

  @Test
  public void testBasicFunctionality() throws Exception {
    EL el = new EL(new EL.DefaultFunctions());
    el.compile("a + b");
    ELResult execute = el.execute(new ELContext().add("a", 1).add("b", 2));
    Assert.assertNotNull(execute);
    Assert.assertEquals(new Integer(3), execute.getInteger());
    Assert.assertEquals(true, el.variables().contains("a"));
    Assert.assertEquals(false, el.variables().contains("c"));
  }

  @Test
  public void testUndefinedVariableException() throws Exception {
    try {
      EL el = new EL(new EL.DefaultFunctions());
      el.compile("a + b + c");
      el.execute(new ELContext().add("a", 1).add("b", 2));
    } catch (ELException e) {
      Assert.assertEquals("Error evaluating expression 'a + b + c'. 'null', at line '1 and column '9'. " +
                            "Input parameters [a(integer) = 1, b(integer) = 2, c(?) = null].", e.getMessage());
    }
  }

  @Test
  public void testSyntaxError() throws Exception {
    try {
      EL el = new EL(new EL.DefaultFunctions());
      el.compile("a +* b + c");
      el.execute(new ELContext().add("a", 1).add("b", 2));
    } catch (ELException e) {
      Assert.assertEquals("Error evaluating expression 'a +* b + c'. (io.cdap.wrangler.expression.EL.compile@1:3 " +
                            "parsing error in '+' at line 1, column 3).", e.getMessage());
    }
  }

  @Test
  public void testNumberFormatError() throws Exception {
    try {
      EL el = new EL(new EL.DefaultFunctions());
      el.compile("a + b + c");
      el.execute(new ELContext().add("c", "a").add("b", 2));
    } catch (ELException e) {
      Assert.assertEquals("Error evaluating expression 'a + b + c + d'. 'null', at line '1 and column '5'. " +
                            "Input parameters [a(?) = null, b(integer) = 2, c(string) = a, d(?) = null].",
                          e.getMessage());
    }
  }

  @Test
  public void testUnknownFunction() throws Exception {
    try {
      EL el = new EL(new EL.DefaultFunctions());
      el.compile("math:maxs(a,b)");
      el.execute(new ELContext().add("a", 1).add("b", 2));
    } catch (ELException e) {
      Assert.assertEquals("Error evaluating expression 'math:maxs(a, b)'. 'Method 'maxs' is not " +
                            "available in namespace', at line '1 and column '10'. Input " +
                            "parameters [a(integer) = 1, b(integer) = 2].", e.getMessage());
    }
  }

  @Test
  public void testMismatchFunction() throws Exception {
    try {
      EL el = new EL(new EL.DefaultFunctions());
      el.compile("math:max(a,b)");
      el.execute(new ELContext().add("a", 1L).add("b", "a"));
    } catch (ELException e) {
      Assert.assertEquals("Error evaluating expression 'math:max(a, b)'. 'Method 'max' doesn't match " +
                            "any signatures [ max(double, double) max(float, float) max(long, long) " +
                            "max(int, int)  ].', at line '1 and column '9'. Input parameters [a(long) = 1, " +
                            "b(string) = a].", e.getMessage());
    }
  }

  @Test
  public void testMismatchTypeCoersion() throws Exception {
    try {
      EL el = new EL(new EL.DefaultFunctions());
      el.compile("math:max(a,b)");
      el.execute(new ELContext().add("a", 1).add("b", 2));
    } catch (ELException e) {
      Assert.assertEquals("Error evaluating expression 'math:max(a, b)'. 'Method 'max' doesn't match " +
                            "any signatures [ max(double, double) max(float, float) max(long, long) " +
                            "max(int, int)  ].', at line '1 and column '9'. Input parameters [a(long) = 1, " +
                            "b(string) = a].", e.getMessage());
    }
  }

  @Test
  public void testArrays() throws Exception {
    EL el = new EL(new EL.DefaultFunctions());
    el.compile("runtime['map'] > token['ABC.EDFG']['input'] " +
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
}
