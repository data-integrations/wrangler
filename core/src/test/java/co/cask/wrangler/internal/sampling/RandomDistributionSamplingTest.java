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

package co.cask.wrangler.internal.sampling;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link RandomDistributionSampling}
 */
public class RandomDistributionSamplingTest {

  @Test
  public void testSampling() throws Exception {
    List<String> records = Lists.newArrayList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    Iterable<String> sampledRecords = Iterables.filter(
      records,
      new RandomDistributionSampling(records.size(), 5)
    );
    List<String> results = Lists.newArrayList(sampledRecords);
    Assert.assertEquals(5, results.size());
  }

}