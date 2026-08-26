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

package io.cdap.wrangler.expression;

import com.google.common.base.Strings;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.functions.DDL;
import io.cdap.functions.DataQuality;
import io.cdap.functions.DateAndTime;
import io.cdap.functions.Dates;
import io.cdap.functions.GeoFences;
import io.cdap.functions.Global;
import io.cdap.functions.JsonFunctions;
import io.cdap.functions.Logical;
import io.cdap.functions.NumberFunctions;
import io.cdap.wrangler.api.JexlAllowlist;
import io.cdap.wrangler.utils.ArithmeticOperations;
import io.cdap.wrangler.utils.DecimalTransform;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlInfo;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.introspection.JexlSandbox;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This class <code>EL</code> is a Expression Language Handler.
 */
public final class EL {

  private static volatile boolean used;

  private final Set<String> variables;
  private final JexlScript script;

  /**
   * Returns {@code true} if this class has been used to execute JEXL script.
   */
  public static boolean isUsed() {
    return used;
  }

  /**
   * Same as calling {@link #compile(ELRegistration, String, CompileOptions)}
   * using
   * {@link DefaultFunctions}.
   * Note: This defaults to allowlist disabled.
   */
  public static EL compile(String expression) throws ELException {
    return compile(new DefaultFunctions(), expression, CompileOptions.getDefaultCompileOptions());
  }

  /**
   * Compiles with specific JEXL allowlist settings.
   *
   * @param expression the JEXL expression
   * @param options    the compilation options
   * @return the compiled EL
   * @throws ELException if failed to compile
   */
  public static EL compile(final String expression, final CompileOptions options) throws ELException {
    return compile(new DefaultFunctions(), expression, options);
  }

  /**
   * Compiles the expression and returns a executable expression.
   *
   * @param registration to be registered with the JEXL context.
   * @param expression   to be compiled.
   * @param options      the compilation options
   * @return a compiled {@link EL} object
   * @throws ELException if failed to compile
   */
  public static EL compile(final ELRegistration registration,
      final String expression,
      final CompileOptions options)
      throws ELException {
    used = true;
    JexlSandbox sandbox = createSandbox(options.getJexlAllowlist(), options.isAllowlistEnabled());
    JexlEngine engine = new JexlBuilder()
      .sandbox(sandbox)
      .namespaces(registration.functions())
      .silent(false)
      .cache(1024)
      .strict(true)
      .logger(new NullLogger())
      .create();

    try {
      JexlScript script = engine.createScript(expression);
      Set<String> variables = extractVariables(script);
      return new EL(script, variables);
    } catch (Exception e) {
      throw handleException(e, expression);
    }
  }

  /**
   * Extracts variables from the script.
   *
   * @param script the script
   * @return the variables
   */
  private static Set<String> extractVariables(final JexlScript script) {
    return script.getVariables().stream()
        .map(vars -> String.join(".", vars))
        .collect(Collectors.toSet());
  }

  private static ELException handleException(Exception e, String expression) {
    if (e instanceof JexlException) {
      JexlException jexlException = (JexlException) e;
      JexlInfo info = jexlException.getInfo();
      return new ELException(
          String.format("Error encountered while evaluating '%s', at line '%d' and column '%d'. " +
              "Make sure the JEXL transformation is valid and uses only allowlisted classes, methods, and properties.",
              info == null || info.getDetail() == null ? expression : info.getDetail(),
              info == null ? 0 : info.getLine(), info == null ? 0 : info.getColumn()),
          e);
    } else if (e instanceof NumberFormatException) {
      return new ELException("Type mismatch. Change type of constant " +
                              "or convert to right data type using conversion functions available. Reason : "
                              + e.getMessage(), e);
    } else {
      if (e.getCause() != null) {
        return new ELException(e.getCause().getMessage(), e);
      } else {
        return new ELException(e);
      }
    }
  }

  /**
   * Creates a JEXL sandbox.
   *
   * @param allowlist       the allowlist
   * @return the sandbox
   */
  public static JexlSandbox createSandbox(
      @Nullable List<JexlAllowlist> allowlist, boolean allowlistEnabled) {
    if (!allowlistEnabled) {
      return null;
    }

    JexlSandbox sandbox = new JexlSandbox(false);
    if (allowlist != null && !allowlist.isEmpty()) {
      allowlist.forEach(rule -> applyAllowlistRule(sandbox, rule));
    }

    return sandbox;
  }

  /**
   * Applies an allowlist rule to the sandbox.
   *
   * @param sandbox the sandbox
   * @param rule    the rule
   */
  private static void applyAllowlistRule(JexlSandbox sandbox, JexlAllowlist rule) {
    String className = rule.getClassName();

    if (rule.isAllMethods() && rule.isAllProperties()) {
      sandbox.white(className);
    } else {
      JexlSandbox.Permissions permissions = sandbox.permissions(className,
          rule.isAllProperties(), rule.isAllProperties(), rule.isAllMethods());

      applyMethodPermissions(rule, permissions);
      applyPropertyPermissions(rule, permissions);
    }
  }

  private static void applyMethodPermissions(JexlAllowlist rule, JexlSandbox.Permissions permissions) {
    if (!rule.isAllMethods()) {
      rule.getMethods().forEach(method -> permissions.execute(method));
    }
  }

  private static void applyPropertyPermissions(JexlAllowlist rule, JexlSandbox.Permissions permissions) {
    if (!rule.isAllProperties()) {
      rule.getProperties().forEach(property -> {
        permissions.read(property);
        permissions.write(property);
      });
    }
  }

  private EL(JexlScript script, Set<String> variables) {
    this.script = script;
    this.variables = Collections.unmodifiableSet(variables);
  }

  public Set<String> variables() {
    return variables;
  }

  public String getScriptParsedText() {
    return script.getParsedText();
  }

  public ELResult execute(ELContext context) throws ELException {
    try {
      // Null the missing fields
      for (String variable : variables) {
        if (!context.has(variable)) {
          context.add(variable, null);
        }
      }
      Object value = script.execute(context);
      return new ELResult(value);
    } catch (Exception e) {
      throw handleException(e, script.getSourceText());
    }
  }

  /**
   * @return List of registered functions.
   */
  public static final class DefaultFunctions implements ELRegistration {
    @Override
    public Map<String, Object> functions() {
      Map<String, Object> functions = new HashMap<>();
      functions.put(null, Global.class);
      functions.put("datetime", DateAndTime.class);
      functions.put("date", Dates.class);
      functions.put("json", JsonFunctions.class);
      functions.put("math", Math.class);
      functions.put("decimal", DecimalTransform.class);
      functions.put("arithmetic", ArithmeticOperations.class);
      functions.put("string", StringUtils.class);
      functions.put("strings", Strings.class);
      functions.put("escape", StringEscapeUtils.class);
      functions.put("bytes", Bytes.class);
      functions.put("arrays", Arrays.class);
      functions.put("dq", DataQuality.class);
      functions.put("ddl", DDL.class);
      functions.put("geo", GeoFences.class);
      functions.put("number", NumberFunctions.class);
      functions.put("logical", Logical.class);
      return functions;
    }

  }

  private static final class NullLogger implements Log {
    @Override
    public void debug(Object o) {

    }

    @Override
    public void debug(Object o, Throwable throwable) {

    }

    @Override
    public void error(Object o) {

    }

    @Override
    public void error(Object o, Throwable throwable) {

    }

    @Override
    public void fatal(Object o) {

    }

    @Override
    public void fatal(Object o, Throwable throwable) {

    }

    @Override
    public void info(Object o) {

    }

    @Override
    public void info(Object o, Throwable throwable) {

    }

    @Override
    public boolean isDebugEnabled() {
      return false;
    }

    @Override
    public boolean isErrorEnabled() {
      return false;
    }

    @Override
    public boolean isFatalEnabled() {
      return false;
    }

    @Override
    public boolean isInfoEnabled() {
      return false;
    }

    @Override
    public boolean isTraceEnabled() {
      return false;
    }

    @Override
    public boolean isWarnEnabled() {
      return false;
    }

    @Override
    public void trace(Object o) {

    }

    @Override
    public void trace(Object o, Throwable throwable) {

    }

    @Override
    public void warn(Object o) {

    }

    @Override
    public void warn(Object o, Throwable throwable) {

    }
  }
}
