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

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.etl.api.StageContext;
import co.cask.wrangler.api.DirectiveInfo;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveRegistry;
import co.cask.wrangler.api.UDD;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A User Directive Registry in a collection of user defined directives. The
 * class <tt>UserDirectiveRegistry</tt> loads the directive either as an
 * {@link ArtifactInfo} or through the use of the context in which it is running.
 *
 * This class provides two constructors for two different context in which the
 * user defined directives are loaded.
 *
 * <p>One context is the service context in which construction of this object
 * would result in investigating all the different artifacts that are of type
 * {@link UDD#Type} and creating a classloader for the same. The classload is
 * then used to create an instance of plugin, in this case it's a directive and
 * extract all the <tt>DirectiveInfo</tt> from the instance of directive created.</p>
 *
 * <p>Second context is the <tt>Transform</tt> plugin, were the second constructor
 * of this class in used to intialize. Initializing this class using <tt>StageContext</tt>
 * provides a way to create an instance of the plugin. The name of the directive is
 * used as the <tt>id</tt> for the plugin.</p>
 *
 * @see SystemDirectiveRegistry
 * @see CompositeDirectiveRegistry
 */
public final class UserDirectiveRegistry implements DirectiveRegistry {
  private final Map<String, DirectiveInfo> registry = new HashMap<>();
  private StageContext context = null;

  /**
   * This constructor should be used when initializing the registry from <tt>Service</tt>.
   *
   * <p><tt>Service</tt> context implements {@link ArtifactManager} interface so it should
   * be readily assignable.</p>
   *
   * <p>Using the <tt>ArtifactManager</tt>, all the artifacts are inspected to check for
   * the artifacts that contain plugins of type <tt>UDD#Type</tt>. For all those plugins,
   * an instance of the plugin is created to extract the annotated and basic information.</p>
   *
   * @param manager an instance of {@link ArtifactManager}.
   * @throws DirectiveLoadException thrown if there are issues loading the plugin.
   */
  public UserDirectiveRegistry(ArtifactManager manager) throws DirectiveLoadException {
    try {
      List<ArtifactInfo> artifacts = manager.listArtifacts();
      for(ArtifactInfo artifact : artifacts) {
        Set<PluginClass> plugins = artifact.getClasses().getPlugins();
        for (PluginClass plugin : plugins) {
          if (UDD.Type.equalsIgnoreCase(plugin.getType())) {
            try(CloseableClassLoader closeableClassLoader = manager.createClassLoader(artifact, null)) {
              Class<? extends UDD> directive =
                (Class<? extends UDD>) closeableClassLoader.loadClass(plugin.getClassName());
              DirectiveInfo classz = new DirectiveInfo(DirectiveInfo.Scope.USER, directive);
              registry.put(classz.name(), classz);
            }
          }
        }
      }
    } catch (IllegalAccessException | InstantiationException | IOException | ClassNotFoundException e) {
      throw new DirectiveLoadException(e.getMessage(), e);
    }
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
  public UserDirectiveRegistry(StageContext context) throws DirectiveLoadException {
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
   * @param name of the directive to be retrived from the registry.
   * @return an instance of {@link DirectiveInfo} if found, else null.
   */
  @Override
  public DirectiveInfo get(String name) throws DirectiveLoadException {
    try {
      if (!registry.containsKey(name)) {
        if (context != null) {
          Class<? extends UDD> directive = context.loadPluginClass(name);
          DirectiveInfo classz = new DirectiveInfo(DirectiveInfo.Scope.USER, directive);
          registry.put(classz.name(), classz);
        }
      } else {
        return registry.get(name);
      }
    } catch (IllegalAccessException | InstantiationException e) {
      throw new DirectiveLoadException(e.getMessage(), e);
    } catch (Exception e) {
      throw new DirectiveLoadException(e.getMessage(), e);
    }
    return null;
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
