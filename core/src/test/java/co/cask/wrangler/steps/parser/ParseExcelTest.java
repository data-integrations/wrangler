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
import co.cask.wrangler.parser.TextDirectives;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link ParseExcel}
 */
public class ParseExcelTest {

  @Test
  public void testBasicExcel() throws Exception {
    try (InputStream stream = ParseAvroFileTest.class.getClassLoader().getResourceAsStream("titanic.xlsx")) {
      byte[] data = IOUtils.toByteArray(stream);

      String[] directives = new String[]{
        "parse-as-excel body 0",
      };

      List<Record> records = new ArrayList<>();
      records.add(new Record("body", data));

      PipelineExecutor executor = new PipelineExecutor();
      executor.configure(new TextDirectives(directives), null);
      List<Record> results = executor.execute(records);
      Assert.assertEquals(892, results.size());
    }
  }

  @Test(expected = PipelineException.class)
  public void testNoSheetName() throws Exception {
    try (InputStream stream = ParseAvroFileTest.class.getClassLoader().getResourceAsStream("titanic.xlsx")) {
      byte[] data = IOUtils.toByteArray(stream);

      String[] directives = new String[]{
        "parse-as-excel body wrong_error",
      };

      List<Record> records = new ArrayList<>();
      records.add(new Record("body", data));

      PipelineExecutor executor = new PipelineExecutor();
      executor.configure(new TextDirectives(directives), null);
      executor.execute(records);
    }
  }
}

