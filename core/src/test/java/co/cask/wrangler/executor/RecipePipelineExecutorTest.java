/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.executor;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.parser.SimpleTextDirectives;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Tests {@link RecipePipelineExecutor}.
 */
public class RecipePipelineExecutorTest {

  @Test
  public void testPipeline() throws Exception {

    String[] commands = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns \"a\",\" b\",'c ',d,e,f,g",
      "rename a first",
      "drop b"
    };
    // Output schema
    Schema schema = Schema.recordOf(
      "output",
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("e", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("f", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("g", Schema.of(Schema.Type.STRING))
    );

    RecipeParser directives =
      new SimpleTextDirectives(StringUtils.join("\n", commands));
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(directives, null);
    Record row = new Record(RecipeParser.STARTING_COLUMN, new String("a,b,c,d,e,f,1.0"));
    StructuredRecord record = (StructuredRecord) pipeline.execute(Arrays.asList(row), schema).get(0);

    // Validate the {@link StructuredRecord}
    Assert.assertEquals("a", record.get("first"));
    Assert.assertEquals("c", record.get("c"));
    Assert.assertEquals("d", record.get("d"));
    Assert.assertEquals("e", record.get("e"));
    Assert.assertEquals("f", record.get("f"));
    Assert.assertEquals("1.0", record.get("g"));
  }


  @Test
  public void testPipelineWithMoreSimpleTypes() throws Exception {

    String[] commands = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns first,last,email,timestamp,weight"
    };
    // Output schema
    Schema schema = Schema.recordOf(
      "output",
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("email", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("weight", Schema.of(Schema.Type.FLOAT))
    );

    RecipeParser directives =
      new SimpleTextDirectives(StringUtils.join("\n", commands));
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(directives, null);
    Record row = new Record(RecipeParser.STARTING_COLUMN, new String("Larry,Perez,lperezqt@umn.edu,1481666448,186.66"));
    StructuredRecord record = (StructuredRecord) pipeline.execute(Arrays.asList(row), schema).get(0);

    // Validate the {@link StructuredRecord}
    Assert.assertEquals("Larry", record.get("first"));
    Assert.assertEquals("Perez", record.get("last"));
    Assert.assertEquals("lperezqt@umn.edu", record.get("email"));
    Assert.assertEquals(1481666448L, record.get("timestamp"));
    Assert.assertEquals(186.66f, record.get("weight"));
  }
}
