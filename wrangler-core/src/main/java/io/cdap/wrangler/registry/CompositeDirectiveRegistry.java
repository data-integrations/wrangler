/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.registry;

import com.google.common.collect.Iterables;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.utils.ArtifactSummaryComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This class implements a Composition of multiple registries.
 *
 * <p>It implements the <tt>DirectiveRegistry</tt> interface. Search for a directives
 * uses the order of the directives specified at the construction time.</p>
 *
 * <p>If the directive is not found in any of the registry, then a <tt>null</tt>
 * is returned.</p>
 */
public final class CompositeDirectiveRegistry implements DirectiveRegistry {
  private final DirectiveRegistry[] registries;

  public CompositeDirectiveRegistry(DirectiveRegistry ... registries) {
    this.registries = registries;
  }

  /**
   * This method looks for the <tt>directive</tt> in all the registered registries.
   *
   * <p>The order of search is as specified by the collection order. Upon finding
   * the first valid instance of directive, the <tt>DirectiveInfo</tt> is returned.</p>
   *
   * @param directive of the directive to be retrived from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  @Nullable
  @Override
  public DirectiveInfo get(String namespace, String directive) throws DirectiveLoadException {
    for (DirectiveRegistry registry : registries) {
      DirectiveInfo info = registry.get(namespace, directive);
      if (info != null) {
        return info;
      }
    }
    return null;
  }

  @Override
  public void reload(String namespace) throws DirectiveLoadException {
    for (DirectiveRegistry registry : registries) {
      registry.reload(namespace);
    }
  }

  @Nullable
  @Override
  public ArtifactSummary getLatestWranglerArtifact() {
    ArtifactSummary latestArtifact = null;
    for (DirectiveRegistry registry : registries) {
      ArtifactSummary artifact = registry.getLatestWranglerArtifact();
      if (artifact == null) {
        continue;
      }
      if (latestArtifact == null) {
        latestArtifact = artifact;
      } else {
        latestArtifact = ArtifactSummaryComparator.pickLatest(latestArtifact, artifact);
      }
    }
    return latestArtifact;
  }

  /**
   * @return Returns an iterator to iterate through all the <code>DirectiveInfo</code> objects
   * maintained within the registry.
   */
  @Override
  public Iterable<DirectiveInfo> list(String namespace) {
    List<Iterable<DirectiveInfo>> lists = new ArrayList<>();
    for (DirectiveRegistry registry : registries) {
      lists.add(registry.list(namespace));
    }
    return Iterables.concat(lists);
  }

  /**
   * Closes any resources acquired during initialization or otherwise.
   */
  @Override
  public void close() throws IOException {
    for (DirectiveRegistry registry : registries) {
      registry.close();
    }
  }
}
