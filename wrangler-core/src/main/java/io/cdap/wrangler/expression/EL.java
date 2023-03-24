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

import com.google.common.base.Joiner;
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
import io.cdap.wrangler.utils.ArithmeticOperations;
import io.cdap.wrangler.utils.DecimalTransform;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlInfo;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
   * Same as calling {@link #compile(ELRegistration, String)} using {@link DefaultFunctions}.
   */
  public static EL compile(String expression) throws ELException {
    return compile(new DefaultFunctions(), expression);
  }

  /**
   * Compiles the given expressions and return an {@link EL} for script execution.
   *
   * @param registration extra objects available for the script to use
   * @param expression the JEXL expresion
   * @return an {@link EL} instance
   * @throws ELException if failed to compile the expression
   */
  public static EL compile(ELRegistration registration, String expression) throws ELException {
    used = true;
    JexlEngine engine = new JexlBuilder()
      .namespaces(registration.functions())
      .silent(false)
      .cache(1024)
      .strict(true)
      .logger(new NullLogger())
      .create();

    try {
      Set<String> variables = new HashSet<>();
      JexlScript script = engine.createScript(expression);
      Set<List<String>> varSet = script.getVariables();
      for (List<String> vars : varSet) {
        variables.add(Joiner.on(".").join(vars));
      }

      return new EL(script, variables);
    } catch (JexlException e) {
      // JexlException.getMessage() uses 'io.cdap.wrangler.expression.EL' class name in the error message.
      // So instead use info object to get information about error message and create custom error message.
      JexlInfo info = e.getInfo();
      throw new ELException(
        String.format("Error encountered while compiling '%s' at line '%d' and column '%d'. " +
                        "Make sure a valid jexl transformation is provided.",
                      // here the detail can be null since there are multiple subclasses which extends this
                      // JexlException, not all of them has this detail information
                      info.getDetail() == null ? expression : info.getDetail(), info.getLine(), info.getColumn()), e);
    } catch (Exception e) {
      throw new ELException(e);
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
    } catch (JexlException e) {
      // JexlException.getMessage() uses 'io.cdap.wrangler.expression.EL' class name in the error message.
      // So instead use info object to get information about error message and create custom error message.
      JexlInfo info = e.getInfo();
      throw new ELException(
        String.format("Error encountered while executing '%s', at line '%d' and column '%d'. " +
                        "Make sure a valid jexl transformation is provided.",
                      // here the detail can be null since there are multiple subclasses which extends this
                      // JexlException, not all of them has this detail information
                      info.getDetail() == null ? script.getSourceText() : info.getDetail(),
                      info.getLine(), info.getColumn()), e);
    } catch (NumberFormatException e) {
      throw new ELException("Type mismatch. Change type of constant " +
                              "or convert to right data type using conversion functions available. Reason : "
                              + e.getMessage(), e);
    } catch (Exception e) {
      if (e.getCause() != null) {
        throw new ELException(e.getCause().getMessage(), e);
      } else {
        throw new ELException(e);
      }
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
