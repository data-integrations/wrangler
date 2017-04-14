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

package co.cask.wrangler;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.PipelineExecutor;
import co.cask.wrangler.internal.TextDirectives;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("SchemaAction")
@Description("An action to perform schema transformation")
public class SchemaAction extends Action {

  private final Config config;

  public SchemaAction(Config config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext actionContext) throws Exception {

    Directives directives = new TextDirectives(config.directives);
    try {
      directives.getSteps();
    } catch (DirectiveParseException e) {
      throw new IllegalArgumentException(e);
    }

    ActionPipelineContext pipelineContext = new ActionPipelineContext(actionContext);
    PipelineExecutor executor = new PipelineExecutor();
    executor.configure(directives, pipelineContext);
    List<Record> result = executor.execute(Collections.singletonList(new Record("schema", config.schema)));

    if (result.isEmpty()) {
      throw new ErrorRecordException("No record left after executing all directives", 1);
    }
    Object schema = result.get(0).getValue("schema");
    if (schema == null) {
      throw new ErrorRecordException("There is no column named \"schema\" after executing all directives.", 1);
    }

    if (!(schema instanceof Schema)) {
      throw new ErrorRecordException("The value of the \"schema\" column must have type " + Schema.class.getName()
                                       + ". Current type is " + schema.getClass().getName(), 1);
    }

    actionContext.getArguments().set(config.avroSchemaName, schema.toString());
    actionContext.getArguments().set(config.schemaName, schema.toString());
  }

  public static final class Config extends PluginConfig {

    public Config(String schema, String directives) {
      this.schema = schema;
      this.directives = directives;
    }

    @Name("schema")
    @Description("Schema to transform")
    @Macro
    private String schema;

    @Name("directives")
    @Description("Directives for transforming the schema. All directives should be operating on the \"schema\" column.")
    @Macro
    private String directives;


    @Description("The argument name for storing the avro schema")
    private String avroSchemaName;

    @Description("The argument name for storing the schema")
    private String schemaName;
  }

  private static final class ActionPipelineContext implements PipelineContext {

    private final ActionContext context;

    private ActionPipelineContext(ActionContext context) {
      this.context = context;
    }

    @Override
    public Environment getEnvironment() {
      return Environment.TRANSFORM;
    }

    @Override
    public StageMetrics getMetrics() {
      return context.getMetrics();
    }

    @Override
    public String getContextName() {
      return context.getStageName();
    }

    @Override
    public Map<String, String> getProperties() {
      return context.getPluginProperties().getProperties();
    }

    @Override
    public <T> Lookup<T> provide(String s, Map<String, String> map) {
      return new Lookup<T>() {
        @Override
        public T lookup(String s) {
          return null;
        }

        @Override
        public Map<String, T> lookup(String... strings) {
          return lookup(Sets.newHashSet(strings));
        }

        @Override
        public Map<String, T> lookup(Set<String> set) {
          Map<String, T> map = new HashMap<>();
          for (String key : set) {
            map.put(key, null);
          }
          return map;
        }
      };
    }
  }
}
