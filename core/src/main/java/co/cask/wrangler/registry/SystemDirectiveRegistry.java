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
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Class description here.
 */
public final class SystemDirectiveRegistry implements  DirectiveRegistry {
  private final Map<String, DirectiveInfo> registry;

  public SystemDirectiveRegistry() {
    registry = new HashMap<>();
    try {
      addUDDefaults();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void addUDDefaults() throws Exception {
    Reflections reflections = new Reflections("co.cask.udd");
    Set<Class<? extends UDD>> system = reflections.getSubTypesOf(UDD.class);
    for(Class<? extends UDD> directive : system) {
      DirectiveInfo classz = new DirectiveInfo(directive);
      registry.put(classz.name(), classz);
    }
  }

  @Override
  public DirectiveInfo get(String name) {
    return registry.get(name);
  }

  @Override
  public Iterator<DirectiveInfo> iterator() {
    return registry.values().iterator();
  }
}
