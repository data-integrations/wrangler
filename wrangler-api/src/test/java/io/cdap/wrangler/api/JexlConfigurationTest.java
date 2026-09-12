/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.wrangler.api;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link JexlConfiguration}.
 */
public class JexlConfigurationTest {

  @Test
  public void testIsJexlAllowlistEnabled() {
    JexlConfiguration trueEnabledConfig = new JexlConfiguration(true, null);
    Assert.assertTrue(trueEnabledConfig.isJexlAllowlistEnabled());

    JexlConfiguration falseEnabledConfig = new JexlConfiguration(false, null);
    Assert.assertFalse(falseEnabledConfig.isJexlAllowlistEnabled());
  }

  @Test
  public void testGetJexlAllowlist() {
    List<JexlAllowlist> allowlists = Collections.singletonList(
        new JexlAllowlist("java.lang.Math", Collections.singletonList("*"), Collections.singletonList("*")));
    JexlConfiguration config = new JexlConfiguration(true, allowlists);
    Assert.assertNotNull(config.getJexlAllowlist());
    Assert.assertEquals(1, config.getJexlAllowlist().size());
    Assert.assertEquals("java.lang.Math", config.getJexlAllowlist().get(0).getClassName());
  }
}
