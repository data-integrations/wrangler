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

package co.cask.wrangler.steps.column;

import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.parser.TextDirectives;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Rename}
 */
public class RenameTest {

  @Test (expected = PipelineException.class)
  public void testRenameOfExistingColumns() throws Exception {
    String[] directives = new String[] {
      "rename C2 C4",
    };

    List<Record> records = Arrays.asList(
      new Record("C1", "A").add("C2", "B").add("C3", "C").add("C4", "D").add("C5", "E")
    );

    TextDirectives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    pipeline.execute(records);
  }
}
