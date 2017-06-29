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

import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveInfo;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveRegistry;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is implementation of {@link DirectiveRegistry} for maintaining a registry
 * of system provided directives. The directives maintained within this registry and
 * present and loaded by the <tt>Classloader</tt> that is responsible for loading this
 * class.
 *
 * <p>In order to load the directives, this class scans through all classes that
 * implement the interface {@link Directive}. Instead of scanning entire JAR, it uses the
 * package name a starting point for scanning the classes that implement the <tt>Directive</tt>
 * interface.</p>
 *
 * <p>For every class found, this scan will create a instance of {@link DirectiveInfo}
 * object and store it in the registry.</p>
 *
 * @see UserDirectiveRegistry
 * @see CompositeDirectiveRegistry
 * @see DirectiveInfo
 */
public final class SystemDirectiveRegistry implements  DirectiveRegistry {
  // This is the default package in which the directives are searched for.
  private static final String PACKAGE = "co.cask.directives";
  private final Map<String, DirectiveInfo> registry;
  private final String namespace;

  public SystemDirectiveRegistry() throws DirectiveLoadException {
    this(PACKAGE);
  }

  /**
   * This constructor uses the user provided <tt>namespace</tt> as starting pointing
   * for scanning classes that implement the interface {@link Directive}.
   *
   * @param namespace that is used as starting point for scanning classes.
   * @throws DirectiveLoadException thrown if there are any issue loading the directive.
   */
  public SystemDirectiveRegistry(String namespace) throws DirectiveLoadException {
    this.registry = new HashMap<>();
    this.namespace = namespace;
    try {
      Reflections reflections = new Reflections(this.namespace);
      Set<Class<? extends Directive>> system = reflections.getSubTypesOf(Directive.class);
      for(Class<? extends Directive> directive : system) {
        DirectiveInfo classz = new DirectiveInfo(DirectiveInfo.Scope.SYSTEM, directive);
        registry.put(classz.name(), classz);
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new DirectiveLoadException(e.getMessage(), e);
    }
  }

  /**
   * Given the name of the directive, returns the information related to the directive.
   *
   * @param name of the directive to be retrived from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  @Override
  public DirectiveInfo get(String name) throws DirectiveLoadException {
    return registry.get(name);
  }

  /**
   * Returns an <tt>JsonElement</tt> representation of this implementation of object.
   * Arrays, Sets are represented as <tt>JsonArray</tt> and other object and map types
   * are represented as <tt>JsonObject</tt>.
   *
   * @return An instance of {@link JsonElement} of this object.
   */
  @Override
  public JsonElement toJson() {
    JsonObject response = new JsonObject();
    for(Map.Entry<String, DirectiveInfo> entry : registry.entrySet()) {
      response.add(entry.getKey(), entry.getValue().toJson());
    }
    return response;
  }
}
