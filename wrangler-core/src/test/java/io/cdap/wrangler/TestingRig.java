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

package io.cdap.wrangler;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.CompileException;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveNotFoundException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.GrammarMigrator;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.parser.SyntaxError;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.parser.RecipeCompiler;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.schema.TransientStoreKeys;
import org.junit.Assert;

import java.util.Iterator;
import java.util.List;

/**
 * Utilities for testing.
 */
public final class TestingRig {

  private TestingRig() {
    // Avoid creation of this object.
  }

  /**
   *
   * @param recipe directives to be executed.
   * @param rows input data
   * @param inputSchema {@link Schema} of the input data
   * @return {@link Schema} of output after transformation
   */
  public static Schema executeAndGetSchema(String[] recipe, List<Row> rows, Schema inputSchema)
    throws DirectiveParseException, DirectiveLoadException, RecipeException {
    ExecutorContext context = new TestingPipelineContext().setSchemaManagementEnabled();
    context.getTransientStore().set(TransientVariableScope.GLOBAL, TransientStoreKeys.INPUT_SCHEMA, inputSchema);
    execute(recipe, rows, context);
    return context.getTransientStore().get(TransientStoreKeys.OUTPUT_SCHEMA);
  }

  /**
   * Executes the directives on the record specified.
   *
   * @param recipe to be executed.
   * @param rows to be executed on directives.
   * @return transformed directives.
   */
  public static List<Row> execute(String[] recipe, List<Row> rows)
    throws RecipeException, DirectiveParseException, DirectiveLoadException {
    return execute(recipe, rows, new TestingPipelineContext());
  }

  public static List<Row> execute(String[] recipe, List<Row> rows, ExecutorContext context)
    throws RecipeException, DirectiveParseException, DirectiveLoadException {
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      SystemDirectiveRegistry.INSTANCE
    );

    String migrate = new MigrateToV2(recipe).migrate();
    RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
    return new RecipePipelineExecutor(parser, context).execute(rows);
  }

  /**
   * Executes the directives on the record specified and returns the results as well as the errors.
   *
   * @param recipe to be executed.
   * @param rows to be executed on directives.
   * @return transformed directives and errors.
   */
  public static Pair<List<Row>, List<Row>> executeWithErrors(String[] recipe, List<Row> rows)
    throws RecipeException, DirectiveParseException, DirectiveLoadException, DirectiveNotFoundException {
    return executeWithErrors(recipe, rows, new TestingPipelineContext());
  }

  public static Pair<List<Row>, List<Row>> executeWithErrors(String[] recipe, List<Row> rows, ExecutorContext context)
    throws RecipeException, DirectiveParseException {
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      SystemDirectiveRegistry.INSTANCE
    );

    String migrate = new MigrateToV2(recipe).migrate();
    RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
    RecipePipeline pipeline = new RecipePipelineExecutor(parser, context);
    List<Row> results = pipeline.execute(rows);
    List<Row> errors = pipeline.errors();
    return new Pair<>(results, errors);
  }

  public static RecipePipeline execute(String[] recipe)
    throws RecipeException, DirectiveParseException, DirectiveLoadException, DirectiveNotFoundException {
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      SystemDirectiveRegistry.INSTANCE
    );

    String migrate = new MigrateToV2(recipe).migrate();
    RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
    return new RecipePipelineExecutor(parser, new TestingPipelineContext());
  }

  public static RecipeParser parse(String[] recipe) throws DirectiveParseException, DirectiveLoadException {
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
      SystemDirectiveRegistry.INSTANCE
    );

    String migrate = new MigrateToV2(recipe).migrate();
    return new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
  }

  public static CompileStatus compile(String[] recipe) throws CompileException, DirectiveParseException {
    GrammarMigrator migrator = new MigrateToV2(recipe);
    Compiler compiler = new RecipeCompiler();
    return compiler.compile(migrator.migrate());
  }

  public static void compileSuccess(String[] recipe) throws CompileException, DirectiveParseException {
    CompileStatus status = compile(recipe);
    Assert.assertTrue(status.isSuccess());
  }

  public static void compileFailure(String[] recipe) throws CompileException, DirectiveParseException {
    CompileStatus status = compile(recipe);
    if (!status.isSuccess()) {
      Iterator<SyntaxError> iterator = status.getErrors();
      while (iterator.hasNext()) {
        System.out.println(iterator.next().toString());
      }
    }
    Assert.assertFalse(status.isSuccess());
  }
}

