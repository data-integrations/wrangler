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

package co.cask.wrangler.optimizer;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.optimizers.UnreachableOptimizer;
import co.cask.wrangler.steps.column.CleanseColumnNames;
import co.cask.wrangler.steps.column.Columns;
import co.cask.wrangler.steps.column.Copy;
import co.cask.wrangler.steps.column.Drop;
import co.cask.wrangler.steps.column.Keep;
import co.cask.wrangler.steps.column.Merge;
import co.cask.wrangler.steps.column.Rename;
import co.cask.wrangler.steps.column.SplitToColumns;
import co.cask.wrangler.steps.column.Swap;
import co.cask.wrangler.steps.date.DiffDate;
import co.cask.wrangler.steps.parser.JsPath;
import co.cask.wrangler.steps.parser.ParseLog;
import co.cask.wrangler.steps.parser.ParseSimpleDate;
import co.cask.wrangler.steps.parser.XmlParser;
import co.cask.wrangler.steps.parser.XmlToJson;
import co.cask.wrangler.steps.transformation.Decode;
import co.cask.wrangler.steps.transformation.Encode;
import co.cask.wrangler.steps.transformation.GenerateUUID;
import co.cask.wrangler.steps.writer.WriteAsJsonMap;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link UnreachableOptimizer}
 */
public class UnreachableOptimizerTest {

  private void doTest(List<Step<Record, Record, String>> expectedOutput, List<Step<Record, Record, String>> input) {
    UnreachableOptimizer unreachableOptimizer = new UnreachableOptimizer();

    List<Step<Record, Record, String>> optimized =
      unreachableOptimizer.optimize(input);

    Assert.assertEquals(expectedOutput, optimized);
  }

  @Test
  public void testEmptyList() throws Exception {
    List<Step<Record, Record, String>> emptyList = ImmutableList.of();
    doTest(emptyList, emptyList);
  }

  @Test
  public void testNoOptimizationsMade() throws Exception {
    List<Step<Record, Record, String>> stepList = ImmutableList.<Step<Record, Record, String>>of(
      new GenerateUUID(1, "makeA", "A"),
      new GenerateUUID(2, "makeB", "B"),
      new GenerateUUID(3, "makeC", "C"),
      new Merge(4, "mergeABtoA", "A", "B", "A", " "),
      new CleanseColumnNames(5, "cleanseColumns"),
      new Merge(6, "mergeCAtoA", "C", "A", "A", " "),
      new Encode(7, "encodeC", Encode.Type.BASE64, "C"),
      new Decode(8, "decodeC", Decode.Type.BASE64, "C"));

    doTest(stepList, stepList);
  }

  @Test
  public void testSimpleOptimizationsMade() throws Exception {
    Step<Record, Record, String> makeA = new GenerateUUID(1, "makeA", "A");
    Step<Record, Record, String> makeB = new GenerateUUID(2, "makeB", "B");
    Step<Record, Record, String> makeC = new GenerateUUID(3, "makeC", "C");
    Step<Record, Record, String> mergeABtoA = new Merge(4, "mergeABtoA", "A", "B", "A", " ");
    Step<Record, Record, String> cleanseColumns = new CleanseColumnNames(5, "cleanseColumns");
    Step<Record, Record, String> mergeBCtoA = new Merge(6, "mergeBCtoA", "B", "C", "A", " ");
    Step<Record, Record, String> encodeC = new Encode(7, "encodeC", Encode.Type.BASE64, "C");
    Step<Record, Record, String> decodeC = new Decode(8, "decodeC", Decode.Type.BASE64, "C");

    List<Step<Record, Record, String>> input =
      ImmutableList.of(makeA, makeB, makeC, mergeABtoA, cleanseColumns, mergeBCtoA, encodeC, decodeC);

    List<Step<Record, Record, String>> expected =
      ImmutableList.of(makeB, makeC, cleanseColumns, mergeBCtoA, encodeC, decodeC);

    doTest(expected, input);
  }

  @Test
  public void testComplexOptimizations() throws Exception {
    Step<Record, Record, String> makeUuid = new GenerateUUID(1, "makeUuid", "UUID");
    Step<Record, Record, String> makeFromXml = new XmlParser(2, "xmlParser", "XML");
    Step<Record, Record, String> merge13to2 = new DiffDate(3, "merge13to2", "XML_1", "XML_3", "XML_2");
    Step<Record, Record, String> cleanseColumns = new CleanseColumnNames(4, "cleanseColumns");
    Step<Record, Record, String> regexSplit = new SplitToColumns(5, "regexSplit", "XML_3", "");
    Step<Record, Record, String> mergeSplitTo2 = new Merge(6, "mergeSplitTo2", "XML_3_1", "XML_3_3", "XML_2", "");
    Step<Record, Record, String> swap12 = new Swap(7, "swap12", "XML_1", "XML_2");
    Step<Record, Record, String> make4from4 = new JsPath(8, "make4from4", "XML_4", "XML_4", "");
    Step<Record, Record, String> make5from4 = new Copy(9, "make5from4", "XML_4", "XML_5", true);
    Step<Record, Record, String> make5From5 = new ParseSimpleDate(10, "make5from5", "XML_5", "");
    Step<Record, Record, String> writeJson = new WriteAsJsonMap(11, "writeJson", "JSON_OUT");
    Step<Record, Record, String> deleteJson = new Drop(12, "deleteJson", "JSON_OUT");
    Step<Record, Record, String> keep12 = new Keep(13, "keep12", new String[]{"XML_1", "XML_2"});

    List<Step<Record, Record, String>> input =
      ImmutableList.of(makeUuid, makeFromXml, merge13to2, cleanseColumns, regexSplit, mergeSplitTo2, swap12, make4from4,
                       make5from4, make5From5, writeJson, deleteJson, keep12);

    List<Step<Record, Record, String>> expected =
      ImmutableList.of(makeFromXml, cleanseColumns, regexSplit, mergeSplitTo2, swap12, deleteJson, keep12);

    doTest(expected, input);
  }

  @Test
  public void testWeirdSteps() throws Exception {
    Step<Record, Record, String> uuidStep = new GenerateUUID(1, "uuidStep", "uuid");
    Step<Record, Record, String> makeFromXml = new XmlParser(2, "xmlParser", "XML");
    Step<Record, Record, String> xmlToJson = new XmlToJson(3, "xmlToJson", "XML", 0);
    Step<Record, Record, String> rename = new Rename(4, "rename", "XML_1", "something");
    Step<Record, Record, String> parseLog = new ParseLog(5, "parseLog", "XML", "something");
    Step<Record, Record, String> dropColumns = new Drop(6, "dropColumns", "XML");
    Step<Record, Record, String> columns = new Columns(7, "columns", ImmutableList.of("XML"));

    List<Step<Record, Record, String>> stepList =
      ImmutableList.of(uuidStep, makeFromXml, xmlToJson, rename, parseLog, dropColumns, columns);

    doTest(stepList, stepList);
  }
}
