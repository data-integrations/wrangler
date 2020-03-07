/*
 *  Copyright © 2020 Cask Data, Inc.
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

package io.cdap.functions;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class LogicalTest {

  @Test
  public void testLogicalBitwiseFunctions() throws Exception {
    String[] directives = new String[]{
      "set-column and logical:BitAnd(352, 400)",
      "set-column or logical:BitOr(352, 400)",
      "set-column xor logical:BitXor(352, 400)",
      "set-column compress logical:BitCompress('0101100000')",
      "set-column expand logical:BitExpand(352)",
      "set-column not1 logical:Not(5-5)",
      "set-column not2 logical:Not(5+5)",
      "set-column not3 logical:Not('value')",
      "set-column bitset logical:SetBit(356,[2,4,8], 1)",
    };

    List<Row> rows = Arrays.asList(new Row("a", 352L).add("b", 400L).add("c","0101100000").add("d", 352));
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);

    short expected = 1;
    Assert.assertEquals(256L, rows.get(0).getValue("and"));
    Assert.assertEquals(496L, rows.get(0).getValue("or"));
    Assert.assertEquals(240L, rows.get(0).getValue("xor"));
    Assert.assertEquals(352L, rows.get(0).getValue("compress"));
    Assert.assertEquals("101100000", rows.get(0).getValue("expand"));
    Assert.assertEquals(expected, rows.get(0).getValue("not1"));
    Assert.assertEquals(expected, rows.get(0).getValue("not2"));
    Assert.assertEquals(expected, rows.get(0).getValue("not3"));
    Assert.assertEquals(494L, rows.get(0).getValue("bitset"));
  }

}