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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.steps.transformation.MessageHash;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link MessageHash}
 */
public class MessageHashTest {

  @Test
  public void testHashBasic() throws Exception {
    String[] directives = new String[] {
      "hash message1 SHA-384 true",
      "hash message2 SHA-384 false",
    };

    List<Row> rows = Arrays.asList(
      new Row("message1", "secret message.")
          .add("message2", "This is a very secret message and a digest will be created.")
    );

    rows = RecipePipelineTest.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
  }

  @Test(expected = DirectiveParseException.class)
  public void testBadAlgorithm() throws Exception {
    String[] directives = new String[] {
      "hash message1 SHA-385 true",
    };

    List<Row> rows = Arrays.asList(
      new Row("message1", "This is a very secret message and a digest will be created.")
    );

    RecipePipelineTest.execute(directives, rows);
  }

}
