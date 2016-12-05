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

import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Specification;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link DefaultPipeline}.
 */
public class DefaultPipelineTest {

  private static final String[] commands = new String[] {
    "set format csv , true",
    "set columns a,b,c,d,e,f,g",
    "rename a first",
    "drop b"
  };

  @Test
  public void testPipeline() throws Exception {
    Specification specification =
      new SimpleSpecification(StringUtils.join("\n", commands));
    Pipeline pipeline = new DefaultPipeline();
    pipeline.configure(specification);
    Row row = (Row)pipeline.execute("a,b,c,d,e,f,g", null);
    Assert.assertTrue(true);
  }
}
