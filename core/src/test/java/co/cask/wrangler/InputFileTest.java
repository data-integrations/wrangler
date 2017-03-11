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

package co.cask.wrangler;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.internal.RecordConvertor;
import co.cask.wrangler.steps.PipelineTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * The test in this file is ignored as we use this only in cases when someone reports an issue with the file.
 */
public class InputFileTest {

  @Ignore
  @Test
  public void testWithFile() throws Exception {
    Path path = Paths.get("<Path-to-File>");
    byte[] data = Files.readAllBytes(path);

    String[] directives = new String[] {
      "find-and-replace body s/\\x00//g",
      "parse-as-csv body ,"
    };

    List<Record> records = Arrays.asList(
      new Record("body", new String(data))
    );
    RecordConvertor convertor = new RecordConvertor();
    records = PipelineTest.execute(directives, records);
    Schema schema = convertor.toSchema("record", records);
    Assert.assertTrue(true);
  }
}
