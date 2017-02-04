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

package co.cask.wrangler.internal;

import co.cask.wrangler.api.MetaAndStatistics;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link BasicMetaAndStatistics}
 */
public class BasicMetaAndStatisticsTest {

  @Test
  public void testMetaBasic() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "1234.45,650-897-3839,111-11-1111,32826,http://www.yahoo.com"),
      new Record("body", "45.56,670-897-3839,111-12-1111,32826,http://mars.io"),
      new Record("body", "45.56,670-897-3839,222 45 6789,32826,http://mars.io")
    );

    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(new TextDirectives(directives), null);
    MetaAndStatistics meta = new BasicMetaAndStatistics();
    records = pipeline.execute(records, meta);

    Assert.assertTrue(records.size() > 1);
  }

}
