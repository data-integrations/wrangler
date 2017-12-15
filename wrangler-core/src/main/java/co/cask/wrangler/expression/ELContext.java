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

package co.cask.wrangler.expression;

import org.apache.commons.jexl3.JexlContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Class description here.
 */
public class ELContext implements JexlContext {
  private final Map<String, Object> values = new HashMap<>();

  public ELContext() {
    // no-op
  }

  public ELContext(String name, Object object) {
    values.put(name, object);
  }

  public ELContext(Map<String, Object> values) {
    this.values.putAll(values);
  }

  @Override
  public Object get(String name) {
    return values.get(name);
  }

  @Override
  public void set(String name, Object value) {
    values.put(name, value);
  }

  public ELContext add(String name, Object value) {
    values.put(name, value);
    return this;
  }


  @Override
  public boolean has(String name) {
    return values.containsKey(name);
  }
}
