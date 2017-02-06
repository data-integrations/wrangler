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

package co.cask.wrangler.steps;

import co.cask.cdap.api.common.Bytes;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.steps.transformation.Expression;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Expression}
 */
public class ExpressionTest {

  @Test
  public void testApplyExpr() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "set column name concat(last, \", \", first)",
      "set column isteen age < 15 ? 'yes' : 'no'",
      "set column salary hrlywage*40*4",
      "drop first",
      "drop last",
      "set column email string:reverse(email)",
      "set column hrlywage var x; x = math:ceil(toFloat(hrlywage)); x + 1",
    };

    // Run through the wrangling steps.
    List<Record> records = Arrays.asList(new Record("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io,32,11.79," +
      "150 Mars Ave,Palo Alto,CA,USA,32826"));

    // Iterate through steps.
    records = PipelineTest.execute(directives, records);

    Assert.assertEquals("Joltie, Root", records.get(0).getValue("name"));
    Assert.assertEquals(1886.3999999999999, records.get(0).getValue("salary"));
    Assert.assertEquals("no", records.get(0).getValue("isteen"));
    Assert.assertEquals("oi.etiloj@toor", records.get(0).getValue("email"));
    Assert.assertEquals(13.0, records.get(0).getValue("hrlywage"));
  }

  @Test(expected = StepException.class)
  public void testNegativeConditionApply() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "set column email string:reverse(email1)" // email1 not defined in the record.
    };

    List<Record> records = Arrays.asList(new Record("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io," +
      "32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"));

    // Run through the wrangling steps.
    PipelineTest.execute(directives, records);
  }

  @Test
  public void testStringNamespace() throws Exception {
    String[] directives = new String[] {
      "set column abbreviate string:abbreviate(longtxt, 5)",
      "set column center string:center(first, 10)",
      "set column eoltxt string:chomp(eoltxt)",
      "set column chop   string:chop(chop)",
      "set column delws  string:deleteWhitespace(delws)"
    };

    List<Record> records = Arrays.asList(
      new Record("number", "1")
            .add("first", "root")
            .add("last", "joltie")
            .add("longtxt", "This is long transformation")
            .add("eoltxt", "This has eol\n")
            .add("chop", "Joltie")
            .add("delws", "Jolti  Root")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("Jolti", records.get(0).getValue("chop"));
    Assert.assertEquals("JoltiRoot", records.get(0).getValue("delws"));
    Assert.assertEquals("This has eol", records.get(0).getValue("eoltxt"));
    Assert.assertEquals("Th...", records.get(0).getValue("abbreviate"));
  }

  @Test
  public void testBytesNamespace() throws Exception {
    String[] directives = new String[] {
      "set column first bytes:toString(first)",
      "set column number bytes:toInt(number)"
    };

    List<Record> records = Arrays.asList(
      new Record("number", Bytes.toBytes(99))
        .add("first", "root".getBytes(StandardCharsets.UTF_8))
        .add("last", "joltie".getBytes(StandardCharsets.UTF_8))
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("root", records.get(0).getValue("first"));
    Assert.assertEquals(99, records.get(0).getValue("number"));
  }

}
