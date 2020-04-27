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

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.http.HttpServiceContext;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveLoadException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.Nullable;

/**
 * A User Executor Registry in a collection of user defined directives. The
 * class <tt>UserDirectiveRegistry</tt> loads the directive either as an
 * {@link ArtifactInfo} or through the use of the context in which it is running.
 *
 * This class provides two constructors for two different context in which the
 * user defined directives are loaded.
 *
 * <p>One context is the service context in which construction of this object
 * would result in investigating all the different artifacts that are of type
 * {@link Directive#TYPE} and creating a classloader for the same. The classload is
 * then used to create an instance of plugin, in this case it's a directive and
 * extract all the <tt>DirectiveInfo</tt> from the instance of directive created.</p>
 *
 * <p>Second context is the <tt>Transform</tt> plugin, were the second constructor
 * of this class in used to initialize. Initializing this class using <tt>StageContext</tt>
 * provides a way to create an instance of the plugin. The name of the directive is
 * used as the <tt>id</tt> for the plugin.</p>
 *
 * @see SystemDirectiveRegistry
 * @see CompositeDirectiveRegistry
 */
public final class UserDirectiveRegistry implements DirectiveRegistry {
  private final Map<String, Map<String, DirectiveInfo>> registry = new ConcurrentSkipListMap<>();
  private final List<CloseableClassLoader> classLoaders = new ArrayList<>();
  private StageContext context = null;
  private HttpServiceContext manager = null;

  /**
   * This constructor should be used when initializing the registry from <tt>Service</tt>.
   *
   * <p><tt>Service</tt> context implements {@link ArtifactManager} interface so it should
   * be readily assignable.</p>
   *
   * <p>Using the <tt>ArtifactManager</tt>, all the artifacts are inspected to check for
   * the artifacts that contain plugins of type <tt>Directive#Type</tt>. For all those plugins,
   * an instance of the plugin is created to extract the annotated and basic information.</p>
   *
   * @param manager an instance of {@link ArtifactManager}.
   * @throws DirectiveLoadException thrown if there are issues loading the plugin.
   */
  public UserDirectiveRegistry(HttpServiceContext manager) throws DirectiveLoadException {
    this.manager = manager;
  }

  /**
   * This constructor is used when constructing this object in the context of <tt>Transform</tt>.
   *
   * A instance of {@link StageContext} is passed to load plugin. <tt>Context</tt> allows
   * loading the plugin from the repository. The directive name is used as the plugin id for
   * loading the class.
   *
   * @param context of <tt>Stage</tt> in <tt>Transform</tt>.
   */
  public UserDirectiveRegistry(StageContext context) {
    this.context = context;
  }

  /**
   * This method provides information about the directive that is being requested.
   *
   * <p>First, the directive is checked for existence with the internal registry.
   * If the directive does not exits in the registry and the <tt>context</tt> is not null, then
   * it's attempted to be loaded as a user plugin. If it does not exist there a null is returned.
   * But, if the plugin exists, then it's loaded and an entry is made into the registry. </p>
   *
   * <p>When invoked through a readable, each plugin is assigned a unique id. The unique
   * id is generated during the <code>configure</code> phase of the plugin. Those ids are
   * passed to initialize through the properties.</p>
   *
   * @param name of the directive to be retrived from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  @Nullable
  @Override
  public DirectiveInfo get(String namespace, String name) throws DirectiveLoadException {
    Class<? extends Directive> directive = null;
    try {
      if (context != null) {
        directive = context.loadPluginClass(name);
      } else {
        if (manager != null) {
          PluginConfigurer configurer = manager.createPluginConfigurer(namespace);
          directive = configurer.usePluginClass(Directive.TYPE, name, UUID.randomUUID().toString(),
                                                PluginProperties.builder().build());
        }
      }
      if (directive == null) {
        throw new DirectiveLoadException(
          String.format("10-5 - Unable to load the user defined directive '%s'. " +
                          "Please check if the artifact containing UDD is still present.", name)
        );
      }
      DirectiveInfo directiveInfo = new DirectiveInfo(DirectiveInfo.Scope.USER, directive);
      return directiveInfo;
    } catch (IllegalAccessException | InstantiationException e) {
      throw new DirectiveLoadException(e.getMessage(), e);
    } catch (IllegalArgumentException e) {
      throw new DirectiveLoadException(
        String.format("Directive '%s' not found. Check if the directive is spelled correctly or artifact " +
                        "containing the directive has been uploaded or you might be missing " +
                        "'#pragma load-directives %s;'", name, name), e
      );
    } catch (Exception e) {
      throw new DirectiveLoadException(e.getMessage(), e);
    }
  }

  @Override
  public void reload(String namespace) throws DirectiveLoadException {
    Map<String, DirectiveInfo> newRegistry = new TreeMap<>();
    Map<String, DirectiveInfo> currentRegistry = registry.computeIfAbsent(namespace, k -> new TreeMap<>());

    if (manager != null) {
      try {
        List<ArtifactInfo> artifacts = manager.listArtifacts(namespace);
        for (ArtifactInfo artifact : artifacts) {
          Set<PluginClass> plugins = artifact.getClasses().getPlugins();
          for (PluginClass plugin : plugins) {
            if (Directive.TYPE.equalsIgnoreCase(plugin.getType())) {
              CloseableClassLoader closeableClassLoader
                    = manager.createClassLoader(namespace, artifact, getClass().getClassLoader());
              Class<? extends Directive> directive =
                (Class<? extends Directive>) closeableClassLoader.loadClass(plugin.getClassName());
              DirectiveInfo classz = new DirectiveInfo(DirectiveInfo.Scope.USER, directive);
              newRegistry.put(classz.name(), classz);
              classLoaders.add(closeableClassLoader);
            }
          }
        }

        MapDifference<String, DirectiveInfo> difference = Maps.difference(currentRegistry, newRegistry);

        // Remove elements from the registry that are not present in newly loaded registry
        for (String directive : difference.entriesOnlyOnLeft().keySet()) {
          currentRegistry.remove(directive);
        }

        // Update common directives
        for (String directive : difference.entriesInCommon().keySet()) {
          currentRegistry.put(directive, difference.entriesInCommon().get(directive));
        }

        // Update new directives
        for (String directive : difference.entriesOnlyOnRight().keySet()) {
          currentRegistry.put(directive, difference.entriesOnlyOnRight().get(directive));
        }
      } catch (IllegalAccessException | InstantiationException | IOException | ClassNotFoundException e) {
        throw new DirectiveLoadException(e.getMessage(), e);
      }
    }
  }

  /**
   * @return Returns an iterator to iterate through all the <code>DirectiveInfo</code> objects
   * maintained within the registry.
   */
  @Override
  public Iterable<DirectiveInfo> list(String namespace) {
    Map<String, DirectiveInfo> namespaceDirectives = registry.getOrDefault(namespace, Collections.emptyMap());
    return namespaceDirectives.values();
  }

  /**
   * Closes any resources acquired during initialization or otherwise.
   */
  @Override
  public void close() throws IOException {
    for (CloseableClassLoader classLoader : classLoaders) {
      classLoader.close();
    }
  }
}
