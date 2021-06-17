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
 *
 */

package io.cdap.wrangler.utils;

import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import org.junit.Assert;
import org.junit.Test;

public class ArtifactSummaryComparatorTest {

  @Test
  public void testArtifactCompare() throws Exception {
    ArtifactSummary summary1 = new ArtifactSummary("wrangler-transform", "1.0.0", ArtifactScope.USER);
    ArtifactSummary summary2 = new ArtifactSummary("wrangler-transform", "1.0.1", ArtifactScope.SYSTEM);
    Assert.assertEquals(summary2, ArtifactSummaryComparator.pickLatest(summary1, summary2));

    summary2 = new ArtifactSummary("wrangler-transform", "1.0.0", ArtifactScope.SYSTEM);
    Assert.assertEquals(summary1, ArtifactSummaryComparator.pickLatest(summary1, summary2));

    summary1 = new ArtifactSummary("wrangler-transform", "2.0.0", ArtifactScope.SYSTEM);
    Assert.assertEquals(summary1, ArtifactSummaryComparator.pickLatest(summary1, summary2));
  }
}
