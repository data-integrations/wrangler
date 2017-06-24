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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.Objects;

/**
 * Class description here.
 */
public final class DirectiveClass {
  public static final int SYSTEM = 0;
  public static final int USER = 1;

  private String name;
  private UsageDefinition definition;
  private Class<?> directive;
  private String usage;
  private String description;

  public DirectiveClass(int type, Class<?> directive) throws IllegalAccessException, InstantiationException {
    this.directive = directive;
    if(type == USER) {
      Object object = directive.newInstance();
      this.definition = ((UDD) object).define();
      this.usage = definition.toString();
    } else if (type == SYSTEM) {
      Usage usage = directive.getAnnotation(Usage.class);
      this.usage = usage.value();
      this.definition = null;
    }
    this.name = directive.getAnnotation(Name.class).value();
    this.description = directive.getAnnotation(Description.class).value();
  }

  public String name() {
    return name;
  }

  public UsageDefinition definition() {
    return definition;
  }

  public UDD instance() throws IllegalAccessException, InstantiationException {
    return (UDD) directive.newInstance();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if(this.getClass() != obj.getClass()) {
      return false;
    }
    final DirectiveClass other = (DirectiveClass) obj;
    return Objects.equals(this.name, other.name);
  }
}