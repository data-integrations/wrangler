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

import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.parser.UsageDefinition;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class description here.
 */
public final class InternalRegistry {
  private final Map<String, DirectiveClass> internal;
  private final Map<String, DirectiveClass> external;

  public InternalRegistry() {
    internal = new HashMap<>();
    external = new HashMap<>();
    try {
      addUDDefaults();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void addUDDefaults() throws Exception {
    Reflections reflections = new Reflections("co.cask.udd");
    Set<Class<? extends UDD>> users = reflections.getSubTypesOf(UDD.class);
    for(Class<? extends UDD> directive : users) {
      DirectiveClass classz = new DirectiveClass(DirectiveClass.USER, directive);
      external.put(classz.name(), classz);
    }

    reflections = new Reflections("co.cask.wrangler");
    Set<Class<? extends AbstractDirective>> system = reflections.getSubTypesOf(AbstractDirective.class);
    for(Class<? extends AbstractDirective> directive : system) {
      DirectiveClass classz = new DirectiveClass(DirectiveClass.SYSTEM, directive);
      internal.put(classz.name(), classz);
    }
  }

  public UsageDefinition definition(String name) {
    return null;
  }

  public Class<?> getDirective() {
    return null;
  }
}
