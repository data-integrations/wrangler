/*
 * Copyright © 2021 Cask Data, Inc.
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


package io.cdap.wrangler.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for reference name util
 */
public class ReferenceNamesTest {

  @Test
  public void testValidation() {
    // valid names
    ReferenceNames.validate("111-22-33.csv");
    ReferenceNames.validate("abc$2.txt");
    ReferenceNames.validate("1$-2.random");

    // invalid names
    testInValidReferenceName("111-22-33(1).csv");
    testInValidReferenceName("1*!.csv");
    testInValidReferenceName("!@#$%^&");
  }

  @Test
  public void testCleanse() {
    // valid names
    Assert.assertEquals("111-22-33.csv", ReferenceNames.cleanseReferenceName("111-22-33.csv"));
    Assert.assertEquals("abc$2.txt", ReferenceNames.cleanseReferenceName("abc$2.txt"));
    Assert.assertEquals("1$-2.random", ReferenceNames.cleanseReferenceName("1$-2.random"));

    // invalid names
    Assert.assertEquals("111-22-331.csv", ReferenceNames.cleanseReferenceName("111-22-33(1).csv"));
    Assert.assertEquals("1.csv", ReferenceNames.cleanseReferenceName("1*!.csv"));
    Assert.assertEquals("$", ReferenceNames.cleanseReferenceName("!@#$%^&"));

    // invalid name with no valid characters
    Assert.assertEquals("sample", ReferenceNames.cleanseReferenceName("!@#%^&*()"));
  }

  private void testInValidReferenceName(String name) {
    try {
      ReferenceNames.validate(name);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
