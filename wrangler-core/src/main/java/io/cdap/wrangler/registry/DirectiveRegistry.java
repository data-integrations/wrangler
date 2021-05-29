/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.wrangler.registry;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.wrangler.api.DirectiveLoadException;

import java.io.Closeable;
import javax.annotation.Nullable;

/**
 * A directive registry maintains a collection of directives either system provided or
 * user provided.
 *
 * <p>The directive information is stored within the registry. The information
 * includes name,class,usage and usage definition.</p>
 *
 * @see DirectiveInfo
 */
public interface DirectiveRegistry extends Closeable {

  /**
   * List the directives in the specified namespace
   *
   * @param namespace the namespace to list from
   * @return directives in the specified namespace
   */
  Iterable<DirectiveInfo> list(String namespace);

  /**
   * Given the name of the directive, returns the information related to the directive.
   *
   * @param namespace the namespace of the directive
   * @param name of the directive to be retrieved from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  @Nullable
  DirectiveInfo get(String namespace, String name) throws DirectiveLoadException;

  /**
   * This method reloads the directives from the artifacts into the registry.
   * Any implementation of this method should provide support for deletes, updates
   * and additions.
   *
   * @param namespace the namespace to reload directives in
   * @throws DirectiveLoadException thrown when there are any issues with loading
   * directives into the registry.
   */
  void reload(String namespace) throws DirectiveLoadException;

  /**
   * Retrieve latest Wrangler transform artifact information
   */
  @Nullable
  ArtifactSummary getLatestWranglerArtifact();

  /**
   * Pick up the latest artifact, this method assumes the name of the artifact is same
   */
  @VisibleForTesting
  static ArtifactSummary pickLatest(ArtifactSummary artifact1, ArtifactSummary artifact2) {
    // first compare the artifact
    int cmp = new ArtifactVersion(artifact1.getVersion()).compareTo(new ArtifactVersion(artifact2.getVersion()));
    if (cmp > 0) {
      return artifact1;
    }

    if (cmp < 0) {
      return artifact2;
    }

    // if scope is differnt, whoever has user scope is latest
    return artifact1.getScope().equals(ArtifactScope.USER) ? artifact1 : artifact2;
  }
}
