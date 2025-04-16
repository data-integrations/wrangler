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
import io.cdap.cdap.api.artifact.ArtifactVersion;

import java.util.Comparator;

/**
 * Comparator for artifact summary
 */
public class ArtifactSummaryComparator implements Comparator<ArtifactSummary> {
  private static final ArtifactSummaryComparator COMPARATOR = new ArtifactSummaryComparator();

  @Override
  public int compare(ArtifactSummary summary1, ArtifactSummary summary2) {
    if (summary1.equals(summary2)) {
      return 0;
    }

    // first compare the artifact
    int cmp = new ArtifactVersion(summary1.getVersion()).compareTo(new ArtifactVersion(summary2.getVersion()));
    if (cmp != 0) {
      return cmp;
    }

    // if scope is different, whoever has user scope is latest
    return summary1.getScope().equals(ArtifactScope.USER) ? 1 : -1;
  }

  /**
   * Pick up the latest artifact, this method assumes the name of the artifact is same
   */
  public static ArtifactSummary pickLatest(ArtifactSummary artifact1, ArtifactSummary artifact2) {
    return COMPARATOR.compare(artifact1, artifact2) > 0 ? artifact1 : artifact2;
  }
}
