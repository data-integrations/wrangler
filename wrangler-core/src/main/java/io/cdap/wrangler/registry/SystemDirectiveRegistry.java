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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveLoadException;
import org.reflections.Reflections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

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
public final class SystemDirectiveRegistry implements DirectiveRegistry {

  public static final SystemDirectiveRegistry INSTANCE;

  static {
    try {
      INSTANCE = new SystemDirectiveRegistry();
    } catch (DirectiveLoadException e) {
      // This shouldn't happen
      throw new RuntimeException("Failed to load system directives", e);
    }
  }

  // This is the default package in which the directives are searched for.
  private static final String PACKAGE = "io.cdap.directives";
  private final Map<String, DirectiveInfo> registry;

  @VisibleForTesting
  SystemDirectiveRegistry() throws DirectiveLoadException {
    this(new ArrayList<>());
  }

  /**
   * This constructor uses the user provided <tt>namespace</tt> as starting pointing
   * for scanning classes that implement the interface {@link Directive}.
   *
   * @param namespaces that is used as starting point for scanning classes.
   * @throws DirectiveLoadException thrown if there are any issue loading the directive.
   */
  public SystemDirectiveRegistry(List<String> namespaces) throws DirectiveLoadException {
    Map<String, DirectiveInfo> registry = new HashMap<>();
    namespaces.add(PACKAGE);
    for (String namespace : namespaces) {
      try {
        Reflections reflections = new Reflections(namespace);
        Set<Class<? extends Directive>> system = reflections.getSubTypesOf(Directive.class);
        for (Class<? extends Directive> directive : system) {
          DirectiveInfo info = DirectiveInfo.fromSystem(directive);
          registry.put(info.name(), info);
        }
      } catch (InstantiationException | IllegalAccessException e) {
        throw new DirectiveLoadException(e.getMessage(), e);
      }
    }
    this.registry = Collections.unmodifiableMap(registry);
  }

  /**
   * Given the name of the directive, returns the information related to the directive.
   *
   * @param name of the directive to be retrieved from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  @Override
  public DirectiveInfo get(String namespace, String name) {
    return get(name);
  }

  /**
   * Given the name of the directive, returns the information related to the directive.
   * This method is specific to system registry as system registry does not need namespace
   * parameter.
   *
   * @param name of the directive to be retrieved from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  public DirectiveInfo get(String name) {
    return registry.get(name);
  }

  @Override
  public void reload(String namespace) {
    // No-op.
  }

  @Nullable
  @Override
  public ArtifactSummary getLatestWranglerArtifact() {
    return null;
  }

  /**
   * @return Returns an iterator to iterate through all the <code>DirectiveInfo</code> objects
   * maintained within the registry.
   */
  @Override
  public Iterable<DirectiveInfo> list(String namespace) {
    return Collections.unmodifiableCollection(registry.values());
  }

  /**
   * Closes any resources acquired during initialization or otherwise.
   */
  @Override
  public void close() {
    // no-op
  }
}
