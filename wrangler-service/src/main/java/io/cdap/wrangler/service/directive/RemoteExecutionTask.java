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
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.SystemAppTaskContext;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.DirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.registry.UserDirectiveRegistry;
import io.cdap.wrangler.utils.ObjectSerDe;

import java.util.List;
import java.util.function.Function;

/**
 * Task for remote execution of directives
 */
public class RemoteExecutionTask implements RunnableTask {

  private static final Gson GSON = new Gson();

  @Override
  public void run(RunnableTaskContext runnableTaskContext) throws Exception {
    RemoteDirectiveRequest directiveRequest = GSON
      .fromJson(runnableTaskContext.getParam(), RemoteDirectiveRequest.class);
    SystemAppTaskContext systemAppContext = runnableTaskContext.getRunnableTaskSystemAppContext();
    String namespace = directiveRequest.getPluginNameSpace();
    try (DirectiveRegistry composite = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry(),
      new UserDirectiveRegistry(systemAppContext))) {
      composite.reload(namespace);
      Function<TransientStore, ExecutorContext> contextProvider =
        store -> new ServicePipelineContext(namespace, ExecutorContext.Environment.SERVICE, systemAppContext, store);
      CommonDirectiveExecutor commonDirectiveExecutor = new CommonDirectiveExecutor(contextProvider, composite);
      ObjectSerDe<List<Row>> objectSerDe = new ObjectSerDe<>();
      List<Row> inputRows = objectSerDe.toObject(directiveRequest.getData());
      List<Row> rows = commonDirectiveExecutor
        .executeDirectives(namespace, directiveRequest.getDirectives(), inputRows);
      runnableTaskContext.writeResult(objectSerDe.toByteArray(rows));
    }
  }
}
