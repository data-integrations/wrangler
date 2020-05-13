/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.mapper;

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.functions.DataQuality;
import io.cdap.functions.Dates;
import io.cdap.functions.Global;
import org.mvel2.MVEL;
import org.mvel2.ParserContext;
import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.MapVariableResolverFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class ExpressionEvaluator {
  private final String script;

  public ExpressionEvaluator(String script) {
    this.script = script;
  }

  public Compiled compile() {
    ParserContext context = new ParserContext();
    context.setStrictTypeEnforcement(false);

    // Add imports
    context.addImport("string", Strings.class);
    context.addImport(Global.class);
    context.addImport(DataQuality.class);
    context.addImport(Dates.class);
    context.addImport("time", MVEL.getStaticMethod(System.class, "currentTimeMillis", new Class[0]));

    // Compile script.
    Serializable serializable = MVEL.compileExpression(script, context);
    return new Compiled(serializable, context);
  }

  /**
   * Stores compiled object.
   */
  public static class Compiled {
    /**
     * Compiled expression.
     */
    private final Serializable compiled;

    /**
     * Context of parser as used in compile phase.
     */
    private final ParserContext context;

    private final RecordMapper mapper;

    private final Map<String, Object> out = new HashMap<>();
    private final Map<String, Object> in = new HashMap<>();
    private final Map<String, Schema> schemas = new HashMap<>();

    public Compiled(Serializable compiled, ParserContext context) {
      this.compiled = compiled;
      this.context = context;
      this.mapper = new RecordMapper();
    }

    public void addOutput(String name, Schema schema) {
      out.put(name, mapper.toMap(schema));
      schemas.put(name, schema);
    }

    public void addInput(String name, StructuredRecord record) {
      in.put(name, mapper.toMap(record));
    }

    /**
     * Executes the expression provided by the user to determine if the record should be selected for processing.
     *
     * @return true if the record should be selected, false otherwise.
     */
    public Result execute() {
      VariableResolverFactory rf = new MapVariableResolverFactory(in);
      rf.setNextFactory(new MapVariableResolverFactory(out));
      Object object = MVEL.executeExpression(compiled, rf);
      return new Result(object, out, schemas);
    }
  }

  public static class Result {
    private final Map<String, Object> out;
    private final Map<String, Schema> schemas;
    private final Object result;
    private final RecordMapper mapper = new RecordMapper();

    public Result(Object result, Map<String, Object> out, Map<String, Schema> schemas) {
      this.result = result;
      this.out = out;
      this.schemas = schemas;
    }

    public Map<String, Object> get(String name) {
      return (Map) out.get(name);
    }

    public Schema getSchema(String name) {
      return schemas.get(name);
    }

    public StructuredRecord getRecord(String name) {
      Schema schema = schemas.get(name);
      if (schema != null) {
        return mapper.fromMap((Map) out.get(name), schema);
      }
      return null;
    }

    public Object result() {
      return result;
    }
  }

}
