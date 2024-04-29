/*
 * Copyright © 2021 Cask Data, Inc.
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
package io.cdap.wrangler.service.directive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.directives.aggregates.DefaultTransientStore;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.CompileException;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRecordBase;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RemoteDirectiveResponse;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import io.cdap.wrangler.parser.DirectiveClass;
import io.cdap.wrangler.parser.GrammarWalker;
import io.cdap.wrangler.parser.MapArguments;
import io.cdap.wrangler.parser.RecipeCompiler;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.ErrorRecordsException;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.UserDirectiveRegistry;
import io.cdap.wrangler.utils.KryoSerializer;
import io.cdap.wrangler.utils.ObjectSerDe;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.cdap.wrangler.schema.TransientStoreKeys.INPUT_SCHEMA;
import static io.cdap.wrangler.schema.TransientStoreKeys.OUTPUT_SCHEMA;

/**
 * Task for remote execution of directives
 */
public class RemoteExecutionTask implements RunnableTask {

  private static final Gson GSON = new GsonBuilder()
          .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
          .create();

  @Override
  public void run(RunnableTaskContext runnableTaskContext) throws Exception {
    RemoteDirectiveRequest directiveRequest = GSON.fromJson(runnableTaskContext.getParam(),
                                                            RemoteDirectiveRequest.class);

    SystemAppTaskContext systemAppContext = runnableTaskContext.getRunnableTaskSystemAppContext();
    String namespace = directiveRequest.getPluginNameSpace();
    Map<String, DirectiveClass> systemDirectives = directiveRequest.getSystemDirectives();
    AtomicBoolean hasUDD = new AtomicBoolean();

    // Collect directives.
    try (UserDirectiveRegistry userDirectiveRegistry = new UserDirectiveRegistry(systemAppContext)) {
      List<Directive> directives = new ArrayList<>();
      GrammarWalker walker = new GrammarWalker(new RecipeCompiler(), new ConfigDirectiveContext(DirectiveConfig.EMPTY));
      walker.walk(directiveRequest.getRecipe(), (command, tokenGroup) -> {
        DirectiveInfo info;
        DirectiveClass directiveClass = systemDirectives.get(command);
        if (directiveClass == null) {
          info = userDirectiveRegistry.get(namespace, command);
          hasUDD.set(true);
        } else {
          // For system directives, we can load it directly from the classloader.
          try {
            info = DirectiveInfo.fromSystem((Class<? extends Directive>) Class.forName(directiveClass.getClassName()));
          } catch (ClassNotFoundException e) {
            throw new DirectiveLoadException("Failed to load system directive " + directiveClass.getName(), e);
          }
        }

        Directive directive = info.instance();
        UsageDefinition definition = directive.define();
        Arguments arguments = new MapArguments(definition, tokenGroup);
        directive.initialize(arguments);
        directives.add(directive);
      });

      // If there is no directives, there is nothing to execute
      if (directives.isEmpty()) {
        runnableTaskContext.writeResult(directiveRequest.getData());
        return;
      }

      ObjectSerDe<List<Row>> objectSerDe = new ObjectSerDe<>();
      List<Row> rows = objectSerDe.toObject(directiveRequest.getData());

      Schema inputSchema = directiveRequest.getInputSchema();
      TransientStore transientStore = new DefaultTransientStore();
      if (inputSchema != null) {
        transientStore.set(TransientVariableScope.GLOBAL, INPUT_SCHEMA, inputSchema);
      }

      try (RecipePipelineExecutor executor = new RecipePipelineExecutor(() -> directives,
                                                                        new ServicePipelineContext(
                                                                          namespace,
                                                                          ExecutorContext.Environment.SERVICE,
                                                                          systemAppContext,
                                                                          transientStore))) {
        rows = executor.execute(rows);
        List<ErrorRecordBase> errors = executor.errors().stream()
            .filter(ErrorRecordBase::isShownInWrangler)
            .collect(Collectors.toList());

        if (!errors.isEmpty()) {
          throw new ErrorRecordsException(errors);
        }
      } catch (RecipeException e) {
        throw new BadRequestException(e.getMessage(), e);
      }

      Schema outputSchema = transientStore.get(OUTPUT_SCHEMA);
      RemoteDirectiveResponse response = new RemoteDirectiveResponse(rows, outputSchema);
      ObjectSerDe<RemoteDirectiveResponse> responseSerDe = new ObjectSerDe<>();

      runnableTaskContext.setTerminateOnComplete(hasUDD.get() || EL.isUsed());

      if (Feature.WRANGLER_KRYO_SERIALIZATION.isEnabled(systemAppContext)) {
        runnableTaskContext.writeResult(new KryoSerializer().fromRemoteDirectiveResponse(response));
      } else {
        runnableTaskContext.writeResult(responseSerDe.toByteArray(response));
      }
    } catch (DirectiveParseException | ClassNotFoundException | CompileException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
  }
}
