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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
    List<FieldOperation> lineage1 = lineage(inputSchema, inputSchema, Collections.emptyList());
    Assert.assertEquals(11, lineage1.size());
  }

  @Test
  public void testNoMapping() throws Exception {
    // No mappings specified
    List<FieldOperation> lineage2 = lineage(inputSchema, outputSchema, Collections.emptyList());
    Assert.assertEquals(0, lineage2.size());
  }

  @Test
  public void testDropLineage() throws Exception {
    List<FieldOperation> lineage4 = lineage(inputSchema, outputSchema, ImmutableList.of("drop a,b,c,d;",
                                                                                        "set-column :o exp:{h}"));
    Assert.assertEquals(3, lineage4.size());
  }

  /*
   * a -------------|
   * b -------------|---> n
   * c -------------|
   */
  @Test
  public void testColumnExpression() throws Exception {
    List<FieldOperation> lineage3 = lineage(inputSchema, outputSchema,
                                            Collections.singletonList("set-column :n exp:{a + b + c + 1}"));
    Assert.assertEquals(4, lineage3.size());
  }

  @Test
  public void testCombination() throws Exception {
    Schema inputSchema = Schema.recordOf("input", Arrays.asList(
      // the parse field
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
      // the identity fields which does not go through any directive
      Schema.Field.of("identity1", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("identity2", Schema.of(Schema.Type.STRING)),
      // drop field
      Schema.Field.of("useless", Schema.of(Schema.Type.STRING)),
      // standard field that go through non-ALL or GENERATE operations
      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c", Schema.of(Schema.Type.STRING))
    ));

    Schema outputSchema = Schema.recordOf("output", Arrays.asList(
      // the parsed fields from "body"
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      // the identity fields
      Schema.Field.of("identity1", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("identity2", Schema.of(Schema.Type.STRING)),
      // a,b,c generates sum
      Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("sum", Schema.of(Schema.Type.STRING))
    ));

    List<String> recipes = ImmutableList.of(
      // parse body to body, body_1, and body_2, drop body and rename other two columns, body_1 and body_2 are actual
      // fields that are not in the input or output schema
      "parse-as-csv :body ',' false",
      "drop body",
      "rename body_1 id",
      "rename body_2 name",
      // regular operations
      "drop useless",
      "set-column :sum exp:{a + b + c}"
    );

    List<FieldOperation> operations = lineage(inputSchema, outputSchema, recipes);

    // should generate 11 operations:
    // parse body
    // drop body
    // rename body_1
    // rename body_2
    // drop useless
    // 4 operations for set-column, one for each field
    // 2 identity transforms, order of identity transforms might not follow the input schema
    Assert.assertEquals(11, operations.size());

    // won't compare directly for the FieldTransformOperation since the output fields are converted from Set, so
    // order might not preserve
    compareFieldOperation(operations, 0, "Parsed column 'body' as CSV with delimiter ','",
                          Collections.singleton("body"),
                          ImmutableSet.of("body_1", "body_2", "id", "name", "sum", "body"));
    compareFieldOperation(operations, 1, "Dropped columns [body]",
                          Collections.singleton("body"), Collections.emptySet());
    compareFieldOperation(operations, 2, "Renamed column 'body_1' to 'id'",
                          Collections.singleton("body_1"), Collections.singleton("id"));
    compareFieldOperation(operations, 3, "Renamed column 'body_2' to 'name'",
                          Collections.singleton("body_2"), Collections.singleton("name"));
    compareFieldOperation(operations, 4, "Dropped columns [useless]",
                          Collections.singleton("useless"), Collections.emptySet());
    compareFieldOperation(operations, 5, "Mapped result of expression 'a + b + c ' to column 'sum'",
                          ImmutableSet.of("a", "b", "c"), Collections.singleton("sum"));
    compareFieldOperation(operations, 6 , "Mapped result of expression 'a + b + c ' to column 'sum'",
                          Collections.singleton("a"), Collections.singleton("a"));
    compareFieldOperation(operations, 7, "Mapped result of expression 'a + b + c ' to column 'sum'",
                          Collections.singleton("b"), Collections.singleton("b"));
    compareFieldOperation(operations, 8, "Mapped result of expression 'a + b + c ' to column 'sum'",
                          Collections.singleton("c"), Collections.singleton("c"));

    // rest two are identity transform, but the order we cannot gaurantee, just make sure input and output fields
    // are identical
    FieldTransformOperation actual = (FieldTransformOperation) operations.get(9);
    Assert.assertEquals("operation_9", actual.getName());
    Assert.assertEquals(actual.getInputFields(), actual.getOutputFields());

    actual = (FieldTransformOperation) operations.get(10);
    Assert.assertEquals("operation_10", actual.getName());
    Assert.assertEquals(actual.getInputFields(), actual.getOutputFields());
  }

  private void compareFieldOperation(List<FieldOperation> operations, int index, String description,
                                     Set<String> expectedInput, Set<String> expectedOutput) {
    FieldTransformOperation actual = (FieldTransformOperation) operations.get(index);
    Assert.assertEquals("operation_" + index, actual.getName());
    Assert.assertEquals(description, actual.getDescription());
    Assert.assertEquals(expectedInput, new HashSet<>(actual.getInputFields()));
    Assert.assertEquals(expectedOutput, new HashSet<>(actual.getOutputFields()));
  }

  private List<FieldOperation> lineage(Schema inputSchema, Schema outputSchema,
                                       List<String> recipes) throws Exception {
    RecipeParser parser = getRecipeParser(recipes);
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

  private RecipeParser getRecipeParser(List<String> directives) throws DirectiveParseException {
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(SystemDirectiveRegistry.INSTANCE);
    return new GrammarBasedParser("default", new MigrateToV2(directives).migrate(), registry);
  }
}
