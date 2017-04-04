/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package co.cask.wrangler.api.i18n;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Messages}
 */
public class MessagesTest {
  private static final Messages MSG = MessagesFactory.getMessages("user/test");
  private static String expected = "this is a simple test";
  @Test
  public void testMessages() throws Exception {
    Assert.assertEquals(expected, MSG.get("test.simple"));
  }

  @Test
  public void testWithOneParameter() throws Exception {
    String value = "this";
    Assert.assertEquals(expected, MSG.get("test.with.1.parameter", value));
  }

}