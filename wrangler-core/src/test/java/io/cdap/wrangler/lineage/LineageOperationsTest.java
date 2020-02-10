/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.wrangler.lineage;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.parser.NoOpDirectiveContext;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test the generation of Lineage defined in {@link LineageOperations}
 */
public class LineageOperationsTest {
  private static Schema inputSchema = Schema.recordOf("input", Arrays.asList(
    Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("e", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("f", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("g", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("h", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("i", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("j", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("k", Schema.of(Schema.Type.STRING))
  ));

  private static Schema outputSchema = Schema.recordOf("output", Arrays.asList(
    Schema.Field.of("n", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("o", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("p", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("q", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("r", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("s", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("t", Schema.of(Schema.Type.STRING))
  ));

  @Test
  public void testIdentityLineage() throws Exception {
    // Identity
    List<FieldOperation> lineage1 = lineage(inputSchema, inputSchema, "");
    Assert.assertEquals(11, lineage1.size());
  }

  @Test
  public void testNoMapping() throws Exception {
    // No mappings specified
    List<FieldOperation> lineage2 = lineage(inputSchema, outputSchema, "");
    Assert.assertEquals(0, lineage2.size());
  }

  @Test
  public void testDropLineage() throws Exception {
    List<FieldOperation> lineage4 = lineage(inputSchema, outputSchema, "drop a,b,c,d;\nset-column :o exp:{h}");
    Assert.assertEquals(3, lineage4.size());
  }

  /*
   * a -------------|
   * b -------------|---> n
   * c -------------|
   */
  @Test
  public void testColumnExpression() throws Exception {
    List<FieldOperation> lineage3 = lineage(inputSchema, outputSchema, "set-column :n exp:{a + b + c + 1}");
    Assert.assertEquals(4, lineage3.size());
  }

  private List<FieldOperation> lineage(Schema inputSchema, Schema outputSchema, String recipe) throws Exception {
    RecipeParser parser = getRecipeParser(recipe);
    List<Directive> directives = parser.parse();
    // After input and output schema are validated, it's time to extract
    // all the fields from input and output schema.
    Set<String> input = inputSchema.getFields().stream()
      .map(Schema.Field::getName).collect(Collectors.toSet());
    // After input and output schema are validated, it's time to extract
    // all the fields from input and output schema.
    Set<String> output = outputSchema.getFields().stream()
      .map(Schema.Field::getName).collect(Collectors.toSet());
    LineageOperations lineageOperations = new LineageOperations(input, output, directives);
    return lineageOperations.generate();
  }

  private RecipeParser getRecipeParser(String directives)
    throws DirectiveLoadException, DirectiveParseException {
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(new SystemDirectiveRegistry());
    RecipeParser recipe = new GrammarBasedParser(
      "default",
      new MigrateToV2(directives).migrate(),
      registry
    );
    recipe.initialize(new NoOpDirectiveContext());
    return recipe;
  }
}
