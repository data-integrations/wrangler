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
 *
 */
package io.cdap.wrangler.service.directive;

import io.cdap.directives.aggregates.DefaultTransientStore;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRecordBase;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.GrammarMigrator;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.ErrorRecordsException;
import io.cdap.wrangler.registry.DirectiveRegistry;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Common code for executing directives
 */
public class CommonDirectiveExecutor {

  private final Function<TransientStore, ExecutorContext> contextProvider;
  private final DirectiveRegistry composite;

  CommonDirectiveExecutor(Function<TransientStore, ExecutorContext> contextProvider, DirectiveRegistry composite) {
    this.contextProvider = contextProvider;
    this.composite = composite;
  }

  /**
   * Execute directives on the provided data sample and return the results
   * @param namespace String namespace
   * @param directives {@link List<String>} of directives to be applied
   * @param sample {@link List<Row>} of sample data
   * @return {@link List<Row>} result after applying the directives
   * @throws DirectiveParseException if there is an issue
   */
  public List<Row> executeDirectives(String namespace, List<String> directives,
                                     List<Row> sample) throws DirectiveParseException {
    RecipePipelineExecutor executor = new RecipePipelineExecutor();
    if (!directives.isEmpty()) {
      GrammarMigrator migrator = new MigrateToV2(directives);
      String migrate = migrator.migrate();
      RecipeParser recipe = new GrammarBasedParser(namespace, migrate, composite);
      recipe.initialize(new ConfigDirectiveContext(DirectiveConfig.EMPTY));
      try {
        executor.initialize(recipe, contextProvider.apply(new DefaultTransientStore()));
        sample = executor.execute(sample);
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
    return sample;
  }
}
