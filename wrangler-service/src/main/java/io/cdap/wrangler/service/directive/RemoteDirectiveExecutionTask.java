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
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskSystemAppContext;
import io.cdap.directives.aggregates.DefaultTransientStore;
import io.cdap.wrangler.api.CompileException;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRecordBase;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.GrammarMigrator;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.RecipeSymbol;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.dataset.workspace.DataType;
import io.cdap.wrangler.dataset.workspace.RequestDeserializer;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.parser.RecipeCompiler;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.ErrorRecordsException;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.Request;
import io.cdap.wrangler.proto.workspace.DirectiveExecutionResponse;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.DirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.registry.UserDirectiveRegistry;
import io.cdap.wrangler.utils.SchemaConverter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 *
 */
public class RemoteDirectiveExecutionTask implements RunnableTask {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Request.class, new RequestDeserializer())
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDirectiveExecutionTask.class);


  @Override
  public void run(RunnableTaskContext runnableTaskContext) throws Exception {
    long sTime = System.nanoTime();
    //runnableTaskContext.writeResult(runnableTaskContext.getParam().getBytes());
    RemoteDirectiveRequest directiveRequest = GSON
      .fromJson(runnableTaskContext.getParam(), RemoteDirectiveRequest.class);
    RunnableTaskSystemAppContext systemAppContext = runnableTaskContext.getSystemAppContext();
    String namespace = directiveRequest.getPluginNameSpace();
    long dirLoadBefore = System.nanoTime();
    DirectiveRegistry composite = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry(),
      new UserDirectiveRegistry(systemAppContext.createPluginConfigurer(namespace),
                                systemAppContext.getArtifactManager())
    );
    LOG.info("Time for loading system directives {}", System.nanoTime() - dirLoadBefore);

    long jsonConvert = System.nanoTime();
    Request userRequest = GSON.fromJson(directiveRequest.getUserRequestString(), Request.class);
    LOG.info("Time for converting req json to object {}", System.nanoTime() - jsonConvert);
    try {
      userRequest.getRecipe().setPragma(addLoadablePragmaDirectives(namespace, userRequest, composite));

      int limit = userRequest.getSampling().getLimit();
      Namespace ns = new Namespace(systemAppContext.getAdmin().getNamespaceSummary(namespace));
      NamespacedId namespacedId = new NamespacedId(ns, directiveRequest.getId());
      Function<List<Row>, List<Row>> function = records -> {
        if (records == null) {
          return Collections.emptyList();
        }
        int min = Math.min(records.size(), limit);
        return records.subList(0, min);
      };

      long tRead = System.nanoTime();
      List<Row> rowList = fromWorkspace(directiveRequest);
      LOG.info("Read data from request {}", System.nanoTime() - tRead);
      long texec = System.nanoTime();
      List<Row> rows = executeDirectives(namespacedId, userRequest, function, composite, rowList,
                                         directiveRequest.getConfig(), systemAppContext);
      LOG.info("Dir execution time {}", System.nanoTime() - texec);
      long tRespObject = System.nanoTime();
      List<Map<String, Object>> values = new ArrayList<>(rows.size());
      Map<String, String> types = new HashMap<>();
      Set<String> headers = new LinkedHashSet<>();
      SchemaConverter convertor = new SchemaConverter();

      // Iterate through all the new rows.
      for (Row row : rows) {
        // If output array has more than return result values, we terminate.
        if (values.size() >= userRequest.getWorkspace().getResults()) {
          break;
        }

        Map<String, Object> value = new HashMap<>(row.width());

        // Iterate through all the fields of the row.
        for (Pair<String, Object> field : row.getFields()) {
          String fieldName = field.getFirst();
          headers.add(fieldName);
          Object object = field.getSecond();

          if (object != null) {
            Schema schema = convertor.getSchema(object, fieldName);
            String type = object.getClass().getSimpleName();
            if (schema != null) {
              schema = schema.isNullable() ? schema.getNonNullable() : schema;
              type = schema.getLogicalType() == null ? schema.getType().name() : schema.getLogicalType().name();
              // for backward compatibility, make the characters except the first one to lower case
              type = type.substring(0, 1).toUpperCase() + type.substring(1).toLowerCase();
            }
            types.put(fieldName, type);
            if ((object.getClass().getMethod("toString").getDeclaringClass() != Object.class)) {
              value.put(fieldName, object.toString());
            } else {
              value.put(fieldName, "Non-displayable object");
            }
          } else {
            value.put(fieldName, null);
          }
        }
        values.add(value);
      }

      DirectiveExecutionResponse directiveExecutionResponse = new DirectiveExecutionResponse(values, headers, types,
                                                                                             userRequest.getRecipe()
                                                                                               .getDirectives());
      LOG.info("Response obj creation time {}", System.nanoTime() - tRespObject);
      long respJSONtime = System.nanoTime();
      String responseJSON = GSON.toJson(directiveExecutionResponse);
      LOG.info("Response json conversion time {}", System.nanoTime() - respJSONtime);
      runnableTaskContext.writeResult(responseJSON.getBytes());
    } catch (JsonParseException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
    LOG.info("Total time in {} {}", this.getClass().getName(), System.nanoTime() - sTime);
  }

  private List<Row> fromWorkspace(RemoteDirectiveRequest remoteDirectiveRequest)
    throws IOException, ClassNotFoundException {
    DataType type = remoteDirectiveRequest.getDatatype();
    List<Row> rows = new ArrayList<>();

    byte[] bytes = remoteDirectiveRequest.getData();
    switch (type) {
      case TEXT: {
        String data = Bytes.toString(bytes);
        if (data != null) {
          rows.add(new Row("body", data));
        }
        break;
      }

      case BINARY: {
        byte[] data = bytes;
        if (data != null) {
          rows.add(new Row("body", data));
        }
        break;
      }

      case RECORDS: {
        if (bytes != null) {
          try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            rows = (List<Row>) ois.readObject();
          }
        }
        break;
      }
    }
    return rows;
  }

  private List<Row> executeDirectives(NamespacedId id, @Nullable Request user,
                                      Function<List<Row>, List<Row>> sample, DirectiveRegistry composite,
                                      List<Row> rows, DirectiveConfig config,
                                      RunnableTaskSystemAppContext systemAppContext) throws
    DirectiveParseException, IOException {
    if (user == null) {
      throw new BadRequestException("Request is empty. Please check if the request is sent as HTTP POST body.");
    }

    TransientStore store = new DefaultTransientStore();
    // Execute the pipeline.
    ExecutorContext context = new ServicePipelineContext(id.getNamespace().getName(),
                                                         ExecutorContext.Environment.SERVICE, systemAppContext, store);
    RecipePipelineExecutor executor = new RecipePipelineExecutor();
    if (user.getRecipe().getDirectives().size() > 0) {
      GrammarMigrator migrator = new MigrateToV2(user.getRecipe().getDirectives());
      String migrate = migrator.migrate();
      RecipeParser recipe = new GrammarBasedParser(id.getNamespace().getName(), migrate, composite);
      recipe.initialize(new ConfigDirectiveContext(config));
      try {
        executor.initialize(recipe, context);
        rows = executor.execute(sample.apply(rows));
      } catch (RecipeException e) {
        throw new BadRequestException(e.getMessage(), e);
      }

      List<ErrorRecordBase> errors = executor.errors()
        .stream()
        .filter(ErrorRecordBase::isShownInWrangler)
        .collect(Collectors.toList());
      if (errors.size() > 0) {
        throw new ErrorRecordsException(errors);
      }

      executor.destroy();
    }
    return rows;
  }

  private String addLoadablePragmaDirectives(String namespace, Request request, DirectiveRegistry composite) {
    StringBuilder sb = new StringBuilder();
    // Validate the DSL by compiling the DSL. In case of macros being
    // specified, the compilation will them at this phase.
    Compiler compiler = new RecipeCompiler();
    try {
      // Compile the directive extracting the loadable plugins (a.k.a
      // Directives in this context).
      CompileStatus status = compiler.compile(new MigrateToV2(request.getRecipe().getDirectives()).migrate());
      RecipeSymbol symbols = status.getSymbols();
      if (symbols == null) {
        return null;
      }

      Iterator<TokenGroup> iterator = symbols.iterator();
      List<String> userDirectives = new ArrayList<>();
      while (iterator.hasNext()) {
        TokenGroup next = iterator.next();
        if (next == null || next.size() < 1) {
          continue;
        }
        Token token = next.get(0);
        if (token.type() == TokenType.DIRECTIVE_NAME) {
          String directive = (String) token.value();
          try {
            DirectiveInfo.Scope scope = composite.get(namespace, directive).scope();
            if (scope == DirectiveInfo.Scope.USER) {
              userDirectives.add(directive);
            }
          } catch (DirectiveLoadException e) {
            // no-op.
          }
        }
      }
      if (userDirectives.size() > 0) {
        sb.append("#pragma load-directives ");
        String directives = StringUtils.join(userDirectives, ",");
        sb.append(directives).append(";");
        return sb.toString();
      }
    } catch (CompileException | DirectiveParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
    return null;
  }

}
