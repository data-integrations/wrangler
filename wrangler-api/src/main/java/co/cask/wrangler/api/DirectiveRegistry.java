/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.api;

import com.google.gson.JsonElement;

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
public interface DirectiveRegistry extends Iterable<DirectiveInfo>, Closeable {
  /**
   * Given the name of the directive, returns the information related to the directive.
   *
   * @param name of the directive to be retrived from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  @Nullable
  DirectiveInfo get(String name) throws DirectiveLoadException;

  /**
   * This method reloads the directives from the artifacts into the registry.
   * Any implementation of this method should provide support for deletes, updates
   * and additions.
   *
   * @throws DirectiveLoadException thrown when there are any issues with loading
   * directives into the registry.
   */
  void reload() throws DirectiveLoadException;

  /**
   * Returns an <tt>JsonElement</tt> representation of this implementation of object.
   * Arrays, Sets are represented as <tt>JsonArray</tt> and other object and map types
   * are represented as <tt>JsonObject</tt>.
   *
   * @return An instance of {@link JsonElement} of this object.
   */
  JsonElement toJson();
}
