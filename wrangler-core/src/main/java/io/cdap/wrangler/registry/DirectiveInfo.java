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

package io.cdap.wrangler.registry;

import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.UsageDefinition;

/**
 * This class <code>DirectiveInfo</code> contains information about each individual
 * directive loaded. It also holds additional information like usage, description,
 * scope,categories and the whether the directive is deprecated or not.
 *
 * Each instance of this class contains information for one directive.
 *
 * @see SystemDirectiveRegistry
 * @see UserDirectiveRegistry
 * @see CompositeDirectiveRegistry
 * @since 3.0
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

  /**
   * Initializes the class <code>DirectiveInfo</code>.
   *
   * @param scope of the directive.
   * @param directive a class of type directive.
   * @throws IllegalAccessException thrown when an application tries to reflectively create an instance
   * @throws InstantiationException Thrown when an application tries to create an instance of a class
   * using the {@code newInstance} method in class {@code Class}
   */
  public DirectiveInfo(Scope scope, Class<? extends Directive> directive)
    throws IllegalAccessException, InstantiationException {
    this.scope = scope;
    this.directive = directive;

    // Create an instance of directive and extract all the annotations
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

  /**
   * @return a <code>Boolean</code> type that indicates if <code>Directive</code> has been deprecated or not.
   */
  public boolean deprecated() {
    return deprecated;
  }

  /**
   * @return a <code>Scope</code> type specifying either USER or SYSTEM scope the directive is deployed in.
   */
  public Scope scope() {
    return scope;
  }

  /**
   * @return a <code>String</code> type specifying the name of the directive.
   */
  public String name() {
    return plugin;
  }

  /**
   * @return a <code>String</code> type containing the usage information of the directive.
   */
  public String usage() {
    return usage;
  }

  /**
   * @return a <code>String</code> type providing the description for a directive.
   */
  public String description() {
    return description;
  }

  /**
   * @return a <code>String</code> type specifying the definition information of directive.
   */
  public UsageDefinition definition() {
    return definition;
  }

  /**
   * @return a <code>String</code> array providing the categories the directive is associated with.
   */
  public String[] categories() {
    return categories;
  }

  /**
   * Creates a new instance of <code>Directive</code>.
   *
   * @return a <code>Directive</code> instance.
   * @throws IllegalAccessException thrown when an application tries to reflectively create an instance
   * @throws InstantiationException Thrown when an application tries to create an instance of a class
   * using the {@code newInstance} method in class {@code Class}
   */
  public Directive instance() throws IllegalAccessException, InstantiationException {
    return (Directive) directive.newInstance();
  }
}
