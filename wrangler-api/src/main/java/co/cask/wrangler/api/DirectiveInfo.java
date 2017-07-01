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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.gson.JsonObject;

/**
 * Class description here.
 */
public final class DirectiveInfo {
  private String name;
  private UsageDefinition definition;
  private Class<?> directive;
  private String usage;
  private String description;
  private Scope scope;
  private boolean deprecated;

  public enum Scope {
    SYSTEM,
    USER
  }

  public DirectiveInfo(Scope scope, Class<?> directive) throws IllegalAccessException, InstantiationException {
    this.scope = scope;
    this.directive = directive;
    Object object = directive.newInstance();
    this.definition = ((Directive) object).define();
    if (definition != null) {
      this.usage = definition.toString();
    } else {
      this.usage = "No definition available for directive '" + directive + "'";
    }
    this.name = directive.getAnnotation(Name.class).value();
    Description desc = directive.getAnnotation(Description.class);
    if (desc == null) {
      this.description = "No description specified for directive class '" + directive.getSimpleName() + "'";
    } else {
      this.description = desc.value();
    }
    Deprecated annotation = directive.getAnnotation(Deprecated.class);
    if (annotation == null) {
      deprecated = false;
    } else {
      deprecated = true;
    }
  }

  public boolean deprecated() {
    return deprecated;
  }

  public Scope scope() {
    return scope;
  }

  public String name() {
    return name;
  }

  public String usage() {
    return usage;
  }

  public String description() { return description; }

  public UsageDefinition definition() {
    return definition;
  }

  public final Directive instance() throws IllegalAccessException, InstantiationException {
    return (Directive) directive.newInstance();
  }

  public final JsonObject toJson() {
    JsonObject response = new JsonObject();
    response.addProperty("plugin", name);
    response.addProperty("usage", usage);
    response.addProperty("description", description);
    response.addProperty("class", directive.getCanonicalName());
    response.add("definition", definition.toJson());
    response.addProperty("scope", scope.name());
    response.addProperty("deprecated", deprecated);
    return response;
  }
}