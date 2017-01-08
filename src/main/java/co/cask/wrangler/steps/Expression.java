/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.StepException;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A Wrangler step for apply expression results to a column.
 */
public class Expression extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(Columns.class);

  // Columns of the column to be upper cased.
  private final String column;

  private final String expression;

  private final JexlEngine engine;

  private final JexlScript script;

  public static class Convertors {
    public static double toDouble(String value) {
      return Double.parseDouble(value);
    }
    public static float toFloat(String value) {
      return Float.parseFloat(value);
    }
    public static long toLong(String value) {
      return Long.parseLong(value);
    }
    public static int toInt(String value) {
      return Integer.parseInt(value);
    }
    public static byte[] toBytes(String value) {
      return value.getBytes();
    }
    public static String concat(String a, String b) {
      return a.concat(b);
    }
    public static String concat(String a, String delim, String b) {
      return a.concat(delim).concat(b);
    }
    public static String trim(String a) {
      return a.trim();
    }

  }

  public Expression(int lineno, String detail, String column, String expression) {
    super(lineno, detail);
    this.column = column;
    this.expression = expression;

    Map<String, Object> functions = new HashMap<>();
    functions.put(null, Convertors.class);
    functions.put("math", Math.class);
    functions.put("string", StringUtils.class);
    engine = new JexlBuilder().namespaces(functions).silent(false).cache(512).strict(true).create();
    script = engine.createScript(expression);
  }

  /**
   * Transforms a column value from any case to upper case.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return Transformed {@link Row} in which the 'col' value is lower cased.
   * @throws StepException thrown when type of 'col' is not STRING.
   */
  @Override
  public Row execute(Row row) throws StepException, SkipRowException {
    Row r = new Row(row);

    JexlContext context = new MapContext();
    for (int i = 0; i < row.length(); ++i) {
      context.set(row.getColumn(i), row.getValue(i));
    }

    try {
      Object result = script.execute(context);
      int idx = r.find(this.column);
      if (idx == -1) {
        r.add(this.column, result.toString());
      } else {
        r.setValue(idx, result.toString());
      }
    } catch (JexlException e) {
      throw new StepException(toString() + " : '" + e.getMessage());
    }
    return r;
  }
}


