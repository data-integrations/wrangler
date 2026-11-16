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
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.JexlAllowlist;
import io.cdap.wrangler.utils.ArithmeticOperations;
import io.cdap.wrangler.utils.DecimalTransform;
import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlInfo;
import org.apache.commons.jexl3.JexlOperator;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.introspection.JexlMethod;
import org.apache.commons.jexl3.introspection.JexlPropertyGet;
import org.apache.commons.jexl3.introspection.JexlPropertySet;
import org.apache.commons.jexl3.introspection.JexlSandbox;
import org.apache.commons.jexl3.introspection.JexlUberspect;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

  private static final Log NULL_LOGGER = new NullLogger();
  private static final JexlUberspect SANDBOXED_UBERSPECT =
      new SandboxedJexlUberspect(new JexlBuilder().logger(NULL_LOGGER).create().getUberspect());

  /**
   * Options applied to every script compiled by this class, scoped to the thread that
   * set them through {@link #initialize(DirectiveContext)}. Defaults to allowlist enabled
   * with {@link io.cdap.wrangler.api.DefaultJexlAllowlist}.
   */
  private static final ThreadLocal<CompileOptions> COMPILE_OPTIONS =
    ThreadLocal.withInitial(CompileOptions::getDefaultCompileOptions);

  private final Set<String> variables;
  private final JexlScript script;
  private final boolean allowlistEnabled;

  /**
   * Returns {@code true} if this class has been used to execute JEXL script.
   */
  public static boolean isUsed() {
    return used;
  }

  /**
   * Initializes the {@link CompileOptions} used by all subsequent calls to
   * {@link #compile(String)} and {@link #compile(ELRegistration, String)}.
   *
   * <p>This is expected to be called once, before the directives of a recipe are
   * initialized, so that the JEXL allowlist configuration does not have to be
   * propagated to every individual directive.
   *
   * @param context the directive context holding the JEXL allowlist configuration.
   */
  public static void initialize(DirectiveContext context) {
    COMPILE_OPTIONS.set(CompileOptions.fromContext(context));
  }

  /**
   * Resets the {@link CompileOptions} of the current thread back to the defaults.
   *
   * <p>Should be called once a thread is done executing a recipe, so that the options
   * are not leaked to the next recipe executed on the same (pooled) thread.
   */
  public static void reset() {
    COMPILE_OPTIONS.remove();
  }

  /**
   * Same as calling {@link #compile(ELRegistration, String)} using
   * {@link DefaultFunctions}.
   *
   * @param expression to be compiled.
   * @return a compiled {@link EL} object
   * @throws ELException if failed to compile
   * @throws ELPermissionException if the expression violates the JEXL sandbox permission rules
   */
  public static EL compile(String expression) throws ELException {
    return compile(new DefaultFunctions(), expression);
  }

  /**
   * Compiles the expression and returns a executable expression.
   *
   * <p>The expression is compiled using the {@link CompileOptions} set through
   * {@link #initialize(DirectiveContext)}.
   *
   * @param registration to be registered with the JEXL context.
   * @param expression   to be compiled.
   * @return a compiled {@link EL} object
   * @throws ELException if failed to compile
   * @throws ELPermissionException if the expression violates the JEXL sandbox permission rules
   */
  public static EL compile(final ELRegistration registration, final String expression)
      throws ELException {
    used = true;
    CompileOptions options = COMPILE_OPTIONS.get();
    boolean allowlistEnabled = options.isAllowlistEnabled();
    JexlSandbox sandbox = createSandbox(options.getJexlAllowlist(), allowlistEnabled);
    JexlBuilder builder = new JexlBuilder()
      .sandbox(sandbox)
      .namespaces(registration.functions())
      .silent(false)
      .cache(1024)
      .strict(true)
      .logger(NULL_LOGGER);
    if (allowlistEnabled) {
      builder.uberspect(SANDBOXED_UBERSPECT);
    }
    JexlEngine engine = builder.create();

    try {
      JexlScript script = engine.createScript(expression);
      Set<String> variables = extractVariables(script);
      return new EL(script, variables, allowlistEnabled);
    } catch (Exception e) {
      throw handleException(e, expression, allowlistEnabled);
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

  private static ELException handleException(
      Exception e, String expression, boolean allowlistEnabled) {
    if (e instanceof JexlException) {
      return handleJexlException((JexlException) e, expression, allowlistEnabled);
    }

    if (e instanceof NumberFormatException) {
      return new ELException("Type mismatch. Change type of constant "
          + "or convert to right data type using conversion functions available. Reason : "
          + e.getMessage(), e);
    }

    return e.getCause() != null
        ? new ELException(e.getCause().getMessage(), e)
        : new ELException(e);
  }

  /**
   * Translates a {@link JexlException} into an {@link ELException}, or throws an
   * {@link ELPermissionException} if the exception represents a JEXL sandbox permission violation.
   *
   * @param e                the JEXL exception to handle
   * @param expression       the source expression being evaluated
   * @param allowlistEnabled whether the JEXL allowlist sandbox is enabled
   * @return an {@link ELException} describing the evaluation error
   * @throws ELPermissionException if the exception is caused by a JEXL sandbox permission violation
   */
  private static ELException handleJexlException(
      JexlException e, String expression, boolean allowlistEnabled) {
    JexlInfo info = e.getInfo();
    Object detail = (info == null || info.getDetail() == null) ? expression : info.getDetail();
    int line = info == null ? 0 : info.getLine();
    int column = info == null ? 0 : info.getColumn();
    String baseErrorMessage = String.format(
        "Error encountered while evaluating '%s', at line '%d' and column '%d'.",
        detail, line, column);

    if (isJexlPermissionViolation(e, allowlistEnabled)) {
      throw new ELPermissionException(
          String.format(
              "%s Make sure the JEXL transformation is valid and uses only allowlisted classes, "
                  + "methods, and properties.",
              baseErrorMessage),
          e);
    }

    return new ELException(
        String.format("%s Make sure the JEXL transformation is valid.", baseErrorMessage), e);
  }

  private static boolean isJexlPermissionViolation(JexlException e, boolean allowlistEnabled) {
    return allowlistEnabled
        && (e instanceof JexlException.Method || e instanceof JexlException.Property);
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

    if (rule.allowAllMethods() && rule.allowAllProperties()) {
      sandbox.white(className);
    } else {
      // configure sandbox permissions: blocklist or allowlist is configured for properties and methods.
      // if blockAllMethods = true, a blocklist is configured to block all methods for the class.
      // if blockAllMethods = false, a allowlist is configured which allows only mentioned methods.
      // Similarly for properties.
      JexlSandbox.Permissions permissions = sandbox.permissions(
          className,
          !rule.blockAllProperties(),
          !rule.blockAllProperties(),
          !rule.blockAllMethods());

      applyMethodPermissions(rule, permissions);
      applyPropertyPermissions(rule, permissions);
    }
  }

  private static void applyMethodPermissions(JexlAllowlist rule, JexlSandbox.Permissions permissions) {
    if (!rule.allowAllMethods() && !rule.blockAllMethods()) {
      rule.getMethods().forEach(method -> permissions.execute(method));
    }
  }

  private static void applyPropertyPermissions(JexlAllowlist rule, JexlSandbox.Permissions permissions) {
    if (!rule.allowAllProperties() && !rule.blockAllProperties()) {
      rule.getProperties().forEach(property -> {
        permissions.read(property);
        permissions.write(property);
      });
    }
  }

  private EL(JexlScript script, Set<String> variables, boolean allowlistEnabled) {
    this.script = script;
    this.variables = Collections.unmodifiableSet(variables);
    this.allowlistEnabled = allowlistEnabled;
  }

  public Set<String> variables() {
    return variables;
  }

  public String getScriptParsedText() {
    return script.getParsedText();
  }

  /**
   * Executes the compiled JEXL script against the given {@link ELContext}.
   *
   * @param context the execution context containing variable bindings
   * @return the {@link ELResult} holding the evaluated value
   * @throws ELException         if an error occurs while evaluating the expression
   * @throws ELPermissionException if the expression violates JEXL sandbox permissions
   */
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
      throw handleException(e, script.getSourceText(), allowlistEnabled);
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

  /**
   * Hardened {@link JexlUberspect} wrapper that blocks {@code Object.getClass()}, {@code .class}
   * property access, constructor invocation ({@code new(...)}), and {@link Class} reflection
   * methods on allowlisted classes and function namespace classes.
   */
  private static final class SandboxedJexlUberspect implements JexlUberspect {
    private static final Set<String> BLOCKED_METHODS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(
            "getClass", "forName", "getClassLoader", "getContextClassLoader",
            "newInstance", "wait", "notify", "notifyAll")));

    private final JexlUberspect delegate;

    private SandboxedJexlUberspect(JexlUberspect delegate) {
      this.delegate = delegate;
    }

    @Override
    public void setClassLoader(ClassLoader loader) {
      delegate.setClassLoader(loader);
    }

    @Override
    public int getVersion() {
      return delegate.getVersion();
    }

    @Override
    public JexlMethod getConstructor(Object ctorHandle, Object... args) {
      return null;
    }

    @Override
    public JexlMethod getMethod(Object obj, String method, Object... args) {
      if (obj == null || method == null || BLOCKED_METHODS.contains(method)) {
        return null;
      }
      int argCount = args == null ? 0 : args.length;
      if (obj instanceof Class<?> && !hasPublicStaticMethod((Class<?>) obj, method, argCount)) {
        return null;
      }
      JexlMethod jexlMethod = delegate.getMethod(obj, method, args);
      if (jexlMethod != null
          && (jexlMethod.getReturnType() == Class.class
              || jexlMethod.getReturnType() == ClassLoader.class)) {
        return null;
      }
      return jexlMethod;
    }

    @Override
    public List<PropertyResolver> getResolvers(JexlOperator op, Object obj) {
      return delegate.getResolvers(op, obj);
    }

    @Override
    public JexlPropertyGet getPropertyGet(Object obj, Object identifier) {
      if (isBlockedPropertyAccess(obj, identifier)) {
        return null;
      }
      return delegate.getPropertyGet(obj, identifier);
    }

    @Override
    public JexlPropertyGet getPropertyGet(List<PropertyResolver> resolvers, Object obj, Object identifier) {
      if (isBlockedPropertyAccess(obj, identifier)) {
        return null;
      }
      return delegate.getPropertyGet(resolvers, obj, identifier);
    }

    @Override
    public JexlPropertySet getPropertySet(Object obj, Object identifier, Object arg) {
      if (isBlockedPropertyAccess(obj, identifier)) {
        return null;
      }
      return delegate.getPropertySet(obj, identifier, arg);
    }

    @Override
    public JexlPropertySet getPropertySet(
        List<PropertyResolver> resolvers, Object obj, Object identifier, Object arg) {
      if (isBlockedPropertyAccess(obj, identifier)) {
        return null;
      }
      return delegate.getPropertySet(resolvers, obj, identifier, arg);
    }

    @Override
    public Iterator<?> getIterator(Object obj) {
      return delegate.getIterator(obj);
    }

    @Override
    public JexlArithmetic.Uberspect getArithmetic(JexlArithmetic arithmetic) {
      return delegate.getArithmetic(arithmetic);
    }

    private static boolean isBlockedPropertyAccess(Object obj, Object identifier) {
      if (obj == null || identifier == null) {
        return true;
      }
      if (obj instanceof Class<?>) {
        return true;
      }
      String prop = identifier.toString();
      return "class".equals(prop) || "TYPE".equals(prop);
    }

    private static boolean hasPublicStaticMethod(Class<?> clazz, String methodName, int argCount) {
      if (clazz == null || clazz == Class.class || clazz == Object.class) {
        return false;
      }
      for (Method m : clazz.getMethods()) {
        if (m.getName().equals(methodName)
            && Modifier.isStatic(m.getModifiers())
            && m.getDeclaringClass() != Class.class
            && m.getDeclaringClass() != Object.class
            && (m.isVarArgs()
                ? argCount >= m.getParameterCount() - 1
                : m.getParameterCount() == argCount)) {
          return true;
        }
      }
      return false;
    }
  }
}
