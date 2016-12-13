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

package co.cask.wrangler.internal;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.Specification;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link DefaultPipeline}.
 */
public class DefaultPipelineTest {

  @Test
  public void testPipeline() throws Exception {

    String[] commands = new String[] {
      "set format csv , true",
      "set columns a,b,c,d,e,f,g",
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

    Specification specification =
      new TextSpecification(StringUtils.join("\n", commands));
    Pipeline pipeline = new DefaultPipeline();
    pipeline.configure(specification);
    StructuredRecord record = (StructuredRecord) pipeline.execute("a,b,c,d,e,f,g", schema);

    // Validate the {@link StructuredRecord}
    Assert.assertEquals("a", record.get("first"));
    Assert.assertEquals("c", record.get("c"));
    Assert.assertEquals("d", record.get("d"));
    Assert.assertEquals("e", record.get("e"));
    Assert.assertEquals("f", record.get("f"));
    Assert.assertEquals("g", record.get("g"));
  }


  @Test
  public void testPipelineWithMoreSimpleTypes() throws Exception {

    String[] commands = new String[] {
      "set format csv , true",
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

    Specification specification =
      new TextSpecification(StringUtils.join("\n", commands));
    Pipeline pipeline = new DefaultPipeline();
    pipeline.configure(specification);
    StructuredRecord record = (StructuredRecord) pipeline.execute("Larry,Perez,lperezqt@umn.edu,1481666448,186.66",
                                                                  schema);

    // Validate the {@link StructuredRecord}
    Assert.assertEquals("Larry", record.get("first"));
    Assert.assertEquals("Perez", record.get("last"));
    Assert.assertEquals("lperezqt@umn.edu", record.get("email"));
    Assert.assertEquals("1481666448", record.get("timestamp"));
    Assert.assertEquals("186.66", record.get("weight"));
  }



}
