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

package co.cask.wrangler.lineage;

import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.Executor;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.lineage.LineageGenerationException;
import co.cask.wrangler.api.lineage.MutationDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LineageTest {
  private void assertBranchingStepNode(FieldLevelLineage.BranchingTransformStepNode node,
                                       int stepNumber, boolean back, boolean forward,
                                       Map<String, Integer> impacting, Map<String,Integer> impacted) {
    Assert.assertEquals(node.getTransformStepNumber(), stepNumber);
    Assert.assertEquals(node.continueBackward(), back);
    Assert.assertEquals(node.continueForward(), forward);
    Assert.assertEquals(node.getImpactingBranches(), impacting);
    Assert.assertEquals(node.getImpactedBranches(), impacted);
  }

  // Optional function to debug lineage trees
  private void print(WranglerFieldLevelLineage lineage, List<String> inputSchema, List<String> outputSchema) {
    System.out.println("\n" + lineage);
    for (String column : inputSchema) {
      System.out.println();
      lineage.prettyPrint(column, true);
    }
    for (String column : outputSchema) {
      System.out.println();
      lineage.prettyPrint(column, false);
    }
  }

  @Test
  public void testBasicModifyLineage() throws Exception {
    String[] directives = new String[] {"lowercase owner"};

    List<String> inputSchema = ImmutableList.of("name", "owner", "species", "sex", "birth", "name2");
    List<String> outputSchema = ImmutableList.of("name", "owner", "species", "sex", "birth", "name2");

    RecipeParser parser = TestingRig.parse(directives);
    List<MutationDefinition> parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    WranglerFieldLevelLineage lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(6, lineage.getLineage().size());
    Assert.assertEquals(1, lineage.getLineage().get("owner").size());
    assertBranchingStepNode(lineage.getLineage().get("owner").get(0), 0, true, true,
                            ImmutableMap.<String, Integer>of(), ImmutableMap.<String, Integer>of());
  }

  @Test
  public void testSimpleLineage() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , false",
      "filter-row-if-matched body_2 ^\\s*$",
      "swap body_2 body_3",
      "rename body_3 name",
      "titlecase name",
      "drop name"
    };

    List<String> inputSchema = ImmutableList.of("body");
    List<String> outputSchema = ImmutableList.of("body", "body_1", "body_2", "body_4", "body_5", "body_6");
    RecipeParser parser = TestingRig.parse(directives);
    List<MutationDefinition> parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    WranglerFieldLevelLineage lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(8, lineage.getLineage().size());
    Assert.assertEquals(2, lineage.getLineage().get("body").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_1").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_2").size());
    Assert.assertEquals(4, lineage.getLineage().get("body_3").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_4").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_5").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_6").size());
    Assert.assertEquals(3, lineage.getLineage().get("name").size());
    Map<String, Integer> impacted = ImmutableMap.<String, Integer>builder()
      .put("body_1", 1)
      .put("body_2", 1)
      .put("body_3", 1)
      .put("body_4", 1)
      .put("body_5", 1)
      .put("body_6", 1)
      .build();
    assertBranchingStepNode(lineage.getLineage().get("body").get(0), 0, true, true,
                            ImmutableMap.<String, Integer>of(), impacted);

    impacted = ImmutableMap.<String, Integer>builder()
      .put("body", 2)
      .put("body_1", 2)
      .put("body_3", 2)
      .put("body_4", 2)
      .put("body_5", 2)
      .put("body_6", 2)
      .build();
    assertBranchingStepNode(lineage.getLineage().get("body_2").get(1), 1, true, true,
                            ImmutableMap.<String, Integer>of(), impacted);
  }

  @Test
  public void testSetColumnsLineage() throws Exception {
    String[] directives = new String[]{
      "parse-as-csv body , false",
      "set columns body,id,fname,creation_time,done,number,date",
      "filter-row-if-matched fname ^\\s*$",
      "swap fname creation_time",
      "rename creation_time name",
      "titlecase name",
      "drop name"
    };
    List<String> inputSchema = ImmutableList.of("body");
    List<String> outputSchema = ImmutableList.of("body", "id", "fname", "done", "number", "date");
    RecipeParser parser = TestingRig.parse(directives);
    List<MutationDefinition> parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    WranglerFieldLevelLineage lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(14, lineage.getLineage().size());
    Assert.assertEquals(3, lineage.getLineage().get("body").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_1").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_2").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_3").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_4").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_5").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_6").size());
    Assert.assertEquals(2, lineage.getLineage().get("id").size());
    Assert.assertEquals(3, lineage.getLineage().get("fname").size());
    Assert.assertEquals(4, lineage.getLineage().get("creation_time").size());
    Assert.assertEquals(2, lineage.getLineage().get("done").size());
    Assert.assertEquals(2, lineage.getLineage().get("number").size());
    Assert.assertEquals(2, lineage.getLineage().get("date").size());
    Assert.assertEquals(3, lineage.getLineage().get("name").size());
    Map<String, Integer> impacted = ImmutableMap.<String, Integer>builder()
      .put("body_1", 1)
      .put("body_2", 1)
      .put("body_3", 1)
      .put("body_4", 1)
      .put("body_5", 1)
      .put("body_6", 1)
      .build();
    assertBranchingStepNode(lineage.getLineage().get("body").get(0), 0,
                            true, true, ImmutableMap.<String, Integer>of(), impacted);
    impacted = ImmutableMap.<String, Integer>builder()
      .put("id", 2)
      .put("body", 3)
      .put("done", 2)
      .put("creation_time", 2)
      .put("number", 2)
      .put("date", 2)
      .build();
    assertBranchingStepNode(lineage.getLineage().get("fname").get(1), 2,
                            true, true, ImmutableMap.<String, Integer>of(), impacted);

    directives = new String[]{
      "parse-as-csv body , false",
      "copy body main",
      "set columns body,name,creation_time,done,number,dob,main",
    };
    inputSchema = ImmutableList.of("body");
    outputSchema = ImmutableList.of("body", "name", "creation_time", "done", "number", "dob", "main");
    parser = TestingRig.parse(directives);
    parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(12, lineage.getLineage().size());
    Assert.assertEquals(3, lineage.getLineage().get("body").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_1").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_2").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_3").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_4").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_5").size());
    Assert.assertEquals(2, lineage.getLineage().get("main").size());
    Assert.assertEquals(1, lineage.getLineage().get("name").size());
    Assert.assertEquals(1, lineage.getLineage().get("creation_time").size());
    Assert.assertEquals(1, lineage.getLineage().get("done").size());
    Assert.assertEquals(1, lineage.getLineage().get("number").size());
    Assert.assertEquals(1, lineage.getLineage().get("dob").size());
    Map<String, Integer> impacting = ImmutableMap.of("main", 0);
    impacted = ImmutableMap.of("main", 2);
    assertBranchingStepNode(lineage.getLineage().get("main").get(1), 2,
                            false, false, impacting, impacted);
    impacted = ImmutableMap.of("name", 1);
    assertBranchingStepNode(lineage.getLineage().get("body_1").get(1), 2,
                            true, false, ImmutableMap.<String, Integer>of(), impacted);
  }

  @Test
  public void testKeepLineage() throws Exception {
    String[] directives = new String[]{
      "parse-as-csv body , false",
      "keep body_1,body_2,body_4,body_6,body_7",
      "set columns id,name,body,num,date",
      "parse-as-json body 1",
      "filter-row-if-matched name ^\\s*$",
      "merge body_1 body_2 body_1_body_2 |",
      "drop body_1,body_2"
    };
    List<String> inputSchema = ImmutableList.of("body");
    List<String> outputSchema = ImmutableList.of("id", "name", "body", "num", "date", "body_1_body_2");
    RecipeParser parser = TestingRig.parse(directives);
    List<MutationDefinition> parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    WranglerFieldLevelLineage lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(11, lineage.getLineage().size());
    Assert.assertEquals(5, lineage.getLineage().get("body").size());
    Assert.assertEquals(7, lineage.getLineage().get("body_1").size());
    Assert.assertEquals(7, lineage.getLineage().get("body_2").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_4").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_6").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_7").size());
    Assert.assertEquals(2, lineage.getLineage().get("id").size());
    Assert.assertEquals(2, lineage.getLineage().get("name").size());
    Assert.assertEquals(2, lineage.getLineage().get("num").size());
    Assert.assertEquals(2, lineage.getLineage().get("date").size());
    Assert.assertEquals(1, lineage.getLineage().get("body_1_body_2").size());
    Map<String, Integer> impacted = ImmutableMap.<String, Integer>builder()
      .put("body", 5)
      .put("body_1", 5)
      .put("body_2", 5)
      .put("id", 2)
      .put("num", 2)
      .put("date", 2)
      .build();
    assertBranchingStepNode(lineage.getLineage().get("name").get(1), 4,
                            true, true, ImmutableMap.<String, Integer>of(), impacted);
    Map<String, Integer> impacting = ImmutableMap.of("body_1", 4, "body_2", 4);
    assertBranchingStepNode(lineage.getLineage().get("body_1_body_2").get(0), 5,
                            false, true, impacting, ImmutableMap.<String, Integer>of());

    directives = new String[]{
      "parse-as-csv body , false",
      "parse-as-csv body_4 \\| true",
      "split-to-columns body_7 \\/",
      "keep body_3,body_5,zero,one,body_7_2",
      "filter-row-if-matched body_3 ^\\s*$",
      "uppercase body_3"
    };
    inputSchema = ImmutableList.of("body");
    outputSchema = ImmutableList.of("body_3", "body_5", "zero", "one", "body_7_2");
    parser = TestingRig.parse(directives);
    parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(8, lineage.getLineage().size());
    Assert.assertEquals(2, lineage.getLineage().get("body").size());
    Assert.assertEquals(4, lineage.getLineage().get("body_3").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_4").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_5").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_7").size());
    Assert.assertEquals(3, lineage.getLineage().get("zero").size());
    Assert.assertEquals(3, lineage.getLineage().get("one").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_7_2").size());
    impacted = ImmutableMap.of("body_3", 1, "body_4", 1, "body_5", 1, "body_7", 1);
    assertBranchingStepNode(lineage.getLineage().get("body").get(0), 0, true, true,
                            ImmutableMap.<String, Integer>of(), impacted);
    impacting = ImmutableMap.of("body_7", 0);
    assertBranchingStepNode(lineage.getLineage().get("body_7_2").get(0), 2, false, true,
                            impacting, ImmutableMap.<String, Integer>of());
  }

  @Test
  public void testParsingLineage() throws Exception {
    String[] directives = new String[]{
      "parse-as-csv body , false",
      "parse-as-csv body_4 \\| true",
      "split-to-columns body_7 \\/",
      "cleanse-column-names",
      "change-column-case upper"
    };
    List<String> inputSchema = ImmutableList.of("body");
    List<String> outputSchema = ImmutableList.of("BODY_3", "BODY_4", "BODY_5", "BODY_7", "ZERO", "ONE", "BODY_7_2");
    RecipeParser parser = TestingRig.parse(directives);
    List<MutationDefinition> parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    WranglerFieldLevelLineage lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(11, lineage.getLineage().size());
    Assert.assertEquals(3, lineage.getLineage().get("body").size());
    Assert.assertEquals(1, lineage.getLineage().get("BODY").size());
    Assert.assertEquals(3, lineage.getLineage().get("BODY_3").size());
    Assert.assertEquals(4, lineage.getLineage().get("body_4").size());
    Assert.assertEquals(1, lineage.getLineage().get("BODY_4").size());
    Assert.assertEquals(3, lineage.getLineage().get("BODY_5").size());
    Assert.assertEquals(4, lineage.getLineage().get("body_7").size());
    Assert.assertEquals(1, lineage.getLineage().get("BODY_7").size());
    Assert.assertEquals(3, lineage.getLineage().get("ZERO").size());
    Assert.assertEquals(3, lineage.getLineage().get("ONE").size());
    Assert.assertEquals(3, lineage.getLineage().get("BODY_7_2").size());
    Map<String, Integer> impacted = ImmutableMap.of("body_4", 1, "BODY_3", 1, "BODY_5", 1, "body_7", 1);
    assertBranchingStepNode(lineage.getLineage().get("body").get(0), 0, true, true,
                            ImmutableMap.<String, Integer>of(), impacted);
    impacted = ImmutableMap.of("BODY_4", 1);
    assertBranchingStepNode(lineage.getLineage().get("body_4").get(3), 4, true, false,
                            ImmutableMap.<String, Integer>of(), impacted);

    directives = new String[]{
      "parse-as-csv body , false",
      "parse-as-csv body_4 \\| true",
      "split-to-columns body_7 \\/"
    };
    inputSchema = ImmutableList.of("body");
    outputSchema = ImmutableList.of("body", "body_1", "body_2", "body_3", "body_4", "body_5", "body_6", "body_7",
                                    "zero", "one", "body_7_1", "body_7_2", "body_7_3");
    parser = TestingRig.parse(directives);
    parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(13, lineage.getLineage().size());
    Assert.assertEquals(1, lineage.getLineage().get("body").size());
    Assert.assertEquals(1, lineage.getLineage().get("body_1").size());
    Assert.assertEquals(1, lineage.getLineage().get("body_2").size());
    Assert.assertEquals(1, lineage.getLineage().get("body_3").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_4").size());
    Assert.assertEquals(1, lineage.getLineage().get("body_5").size());
    Assert.assertEquals(1, lineage.getLineage().get("body_6").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_7").size());
    Assert.assertEquals(1, lineage.getLineage().get("zero").size());
    Assert.assertEquals(1, lineage.getLineage().get("one").size());
    Assert.assertEquals(1, lineage.getLineage().get("body_7_1").size());
    Assert.assertEquals(1, lineage.getLineage().get("body_7_2").size());
    Assert.assertEquals(1, lineage.getLineage().get("body_7_3").size());
    impacted = ImmutableMap.of("one", 1, "zero", 1);
    assertBranchingStepNode(lineage.getLineage().get("body_4").get(1), 1, true, true,
                            ImmutableMap.<String, Integer>of(), impacted);
    impacted = ImmutableMap.of("body_7_1", 1, "body_7_2", 1, "body_7_3", 1);
    assertBranchingStepNode(lineage.getLineage().get("body_7").get(1), 2, true, true,
                            ImmutableMap.<String, Integer>of(), impacted);
  }

  @Test
  public void testComprehensiveLineage() throws Exception {
    String[] directives = new String[]{
      "parse-as-csv body , false",
      "parse-as-simple-date body_6 MM/dd/yyyy",
      "fill-null-or-empty body_5 4",
      "set columns body,id,name,creation_time,done,number,dob",
      "drop body,number",
      "filter-rows-on empty-or-null-columns name,creation_time",
      "parse-as-date creation_time PST",
      "drop creation_time",
      "rename creation_time_1 creation_time"
    };
    List<String> inputSchema = ImmutableList.of("body");
    List<String> outputSchema = ImmutableList.of("id", "name", "done", "dob", "creation_time");
    RecipeParser parser = TestingRig.parse(directives);
    List<MutationDefinition> parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    WranglerFieldLevelLineage lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(14, lineage.getLineage().size());
    Assert.assertEquals(3, lineage.getLineage().get("body").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_1").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_2").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_3").size());
    Assert.assertEquals(2, lineage.getLineage().get("body_4").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_5").size());
    Assert.assertEquals(3, lineage.getLineage().get("body_6").size());
    Assert.assertEquals(2, lineage.getLineage().get("id").size());
    Assert.assertEquals(2, lineage.getLineage().get("name").size());
    Assert.assertEquals(5, lineage.getLineage().get("creation_time").size());
    Assert.assertEquals(2, lineage.getLineage().get("done").size());
    Assert.assertEquals(2, lineage.getLineage().get("number").size());
    Assert.assertEquals(2, lineage.getLineage().get("dob").size());
    Assert.assertEquals(2, lineage.getLineage().get("creation_time_1").size());
    Map<String, Integer> impacted = ImmutableMap.of("id", 2, "dob", 2, "done", 2);
    assertBranchingStepNode(lineage.getLineage().get("name").get(1), 5, true, true,
                            ImmutableMap.<String, Integer>of(), impacted);
    Map<String, Integer> impacting = ImmutableMap.of("name", 0, "creation_time", 0);
    assertBranchingStepNode(lineage.getLineage().get("id").get(1), 5, true, true,
                            impacting, ImmutableMap.<String, Integer>of());
    impacting = ImmutableMap.of("creation_time_1", 0);
    assertBranchingStepNode(lineage.getLineage().get("creation_time").get(4), 8, false, true,
                            impacting, ImmutableMap.<String, Integer>of());
  }

  @Test
  public void testRandomColumnsLineage() throws Exception {
    String[] directives = new String[]{
      "parse-as-json body 1",
      "set columns body,one,two"
    };

    List<String> inputSchema = ImmutableList.of("body");
    List<String> outputSchema = ImmutableList.of("body", "one", "two");
    RecipeParser parser = TestingRig.parse(directives);
    List<MutationDefinition> parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    WranglerFieldLevelLineage lineage = (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
    Assert.assertEquals(5, lineage.getLineage().size());
    Assert.assertEquals(2, lineage.getLineage().get("body").size());
    Assert.assertEquals(2, lineage.getLineage().get("random_column_1").size());
    Assert.assertEquals(2, lineage.getLineage().get("random_column_2").size());
    Assert.assertEquals(1, lineage.getLineage().get("one").size());
    Assert.assertEquals(1, lineage.getLineage().get("two").size());
    Map<String, Integer> impacted = ImmutableMap.of("random_column_1", 1, "random_column_2", 1);
    assertBranchingStepNode(lineage.getLineage().get("body").get(0), 0, true, true,
                            ImmutableMap.<String, Integer>of(), impacted);
    Map<String, Integer> impacting = ImmutableMap.of("random_column_2", 0);
    assertBranchingStepNode(lineage.getLineage().get("two").get(0), 1, false, true,
                            impacting, ImmutableMap.<String, Integer>of());
  }

  @Test(expected = LineageGenerationException.class)
  public void testBroken() throws Exception {
    String[] directives = new String[]{
      "parse-as-csv body , false",
      "split-to-columns body_7 \\/",
      "rename body_mahalo great \\/"
    };

    List<String> inputSchema = ImmutableList.of("body");
    List<String> outputSchema = ImmutableList.of("body", "body_7", "body_7_1", "great");
    RecipeParser parser = TestingRig.parse(directives);
    List<MutationDefinition> parseTree = new ArrayList<>();
    for (Executor directive : parser.parse()) {
      parseTree.add(directive.lineage());
    }
    (new WranglerFieldLevelLineage.Builder(inputSchema, outputSchema)).build(parseTree);
  }
}
