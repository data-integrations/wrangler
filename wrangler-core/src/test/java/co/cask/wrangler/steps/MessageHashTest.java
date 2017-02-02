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
import co.cask.wrangler.api.Record;
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

    List<Record> records = Arrays.asList(
      new Record("message1", "secret message.")
          .add("message2", "This is a very secret message and a digest will be created.")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals("cfe78f0c7bf4b1e5838acc29a453fb89e3e360cb17325823b82e0951" +
                          "71d8460804fecb46461cee27bcecc6af7fa9a47a", records.get(0).getValue(0));
  }

  @Test(expected = DirectiveParseException.class)
  public void testBadAlgorithm() throws Exception {
    String[] directives = new String[] {
      "hash message1 SHA-385 true",
    };

    List<Record> records = Arrays.asList(
      new Record("message1", "This is a very secret message and a digest will be created.")
    );

    PipelineTest.execute(directives, records);
  }

}