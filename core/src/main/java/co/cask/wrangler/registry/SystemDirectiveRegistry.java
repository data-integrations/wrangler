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

package co.cask.wrangler.registry;

import co.cask.wrangler.api.UDD;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A registry for aggregating all the system provided directives.
 *
 * <p>
 *   This class inspects the classes that implement the interface {@link UDD}
 *   to extract information about them and maintain them in the registry.
 * </p>
 */
public final class SystemDirectiveRegistry implements  DirectiveRegistry {
  private static final String PACKAGE = "co.cask.udd";
  private final Map<String, DirectiveInfo> registry;
  private final String pkg;

  public SystemDirectiveRegistry() throws DirectiveLoadException {
    this(PACKAGE);
  }

  public SystemDirectiveRegistry(String pkg) throws DirectiveLoadException {
    this.registry = new HashMap<>();
    this.pkg = pkg;
    try {
      loadSystemDirectives();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new DirectiveLoadException(e.getMessage(), e);
    }
  }

  /**
   * Loads the directives embedded within the system into a registry.
   */
  private void loadSystemDirectives() throws InstantiationException, IllegalAccessException {
    Reflections reflections = new Reflections(pkg);
    Set<Class<? extends UDD>> system = reflections.getSubTypesOf(UDD.class);
    for(Class<? extends UDD> directive : system) {
      DirectiveInfo classz = new DirectiveInfo(directive);
      registry.put(classz.name(), classz);
    }
  }

  /**
   * Given the name of the directive, returns the information related to the directive.
   *
   * @param name of the directive to be retrived from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  @Override
  public DirectiveInfo get(String name) throws DirectiveLoadException, DirectiveNotFoundException {
    return registry.get(name);
  }

  /**
   * @return Iterator to all the directives held within the directive registry.
   */
  @Override
  public Iterator<DirectiveInfo> iterator() {
    return registry.values().iterator();
  }

  @Override
  public JsonElement toJson() {
    JsonObject response = new JsonObject();
    for(Map.Entry<String, DirectiveInfo> entry : registry.entrySet()) {
      response.add(entry.getKey(), entry.getValue().toJson());
    }
    return response;
  }
}
