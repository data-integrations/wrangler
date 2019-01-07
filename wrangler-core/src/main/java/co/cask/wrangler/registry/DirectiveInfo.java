/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.registry;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.gson.annotations.SerializedName;

/**
 * This class <code>DirectiveInfo</code> contains information about each individual
 * directive loaded. It also holds additional information like usage, description,
 * scope,categories and the whether the directive is deprecated or not.
 *
 * Each instance of this class contains information for one directive.
 */
public final class DirectiveInfo {
  private final String plugin;
  private final String usage;
  private final String description;
  @SerializedName("class")
  private final String directiveClass;
  private final UsageDefinition definition;
  private final Scope scope;
  private final boolean deprecated;
  private final String[] categories;
  // transient so it doesn't get included in the REST responses
  private final transient Class<?> directive;

  /**
   * Scope of the directive
   */
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
    this.plugin = directive.getAnnotation(Name.class).value();
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
    Categories category = directive.getAnnotation(Categories.class);
    if (category == null) {
      categories = new String[] { "default" };
    } else {
      categories = category.categories();
    }
    directiveClass = directive.getCanonicalName();
  }

  public boolean deprecated() {
    return deprecated;
  }

  public Scope scope() {
    return scope;
  }

  public String name() {
    return plugin;
  }

  public String usage() {
    return usage;
  }

  public String description() {
    return description;
  }

  public UsageDefinition definition() {
    return definition;
  }

  public String[] categories() {
    return categories;
  }

  public Directive instance() throws IllegalAccessException, InstantiationException {
    return (Directive) directive.newInstance();
  }
}
