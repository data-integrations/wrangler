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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.executor.TextDirectives;
import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link ParseAvroFile}
 */
public class ParseAvroFileTest {

  @Test
  public void testParseAsAvroFile() throws Exception {
    InputStream stream = ParseAvroFileTest.class.getClassLoader().getResourceAsStream("cdap-log.avro");
    byte[] data = IOUtils.toByteArray(stream);

    String[] directives = new String[] {
      "parse-as-avro-file body",
    };

    List<Record> records = new ArrayList<>();
    records.add(new Record("body", data));

    PipelineExecutor executor = new PipelineExecutor();
    executor.configure(new TextDirectives(directives), null);
    List<Record> results = executor.execute(records);
    Assert.assertEquals(1689, results.size());
    Assert.assertEquals(15, results.get(0).length());
    Assert.assertEquals(1495172588118L, results.get(0).getValue("timestamp"));
    Assert.assertEquals(1495194308245L, results.get(1688).getValue("timestamp"));
  }

  @Test(expected = PipelineException.class)
  public void testIncorrectType() throws Exception {
    String[] directives = new String[] {
      "parse-as-avro-file body",
    };

    List<Record> records = new ArrayList<>();
    records.add(new Record("body", new String("failure").getBytes(Charsets.UTF_8)));
    PipelineExecutor executor = new PipelineExecutor();
    executor.configure(new TextDirectives(directives), null);
    executor.execute(records);
  }

}