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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.parser.DirectiveClass;

import javax.annotation.Nullable;

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
  private final DirectiveClass directiveClass;
  private final String description;
  private final boolean deprecated;
  private final String[] categories;
  private final Class<? extends Directive> directive;
  private volatile boolean definitionLoaded;
  private UsageDefinition definition;
  private volatile String usage;

  /**
   * Creates a {@link DirectiveInfo} of the given class coming from the {@link DirectiveScope#SYSTEM} scope.
   */
  public static DirectiveInfo fromSystem(Class<? extends Directive> cls)
    throws InstantiationException, IllegalAccessException {
    return new DirectiveInfo(DirectiveScope.SYSTEM, cls, null);
  }

  /**
   * Creates a {@link DirectiveInfo} of the given class coming from the {@link DirectiveScope#USER} scope.
   */
  public static DirectiveInfo fromUser(Class<? extends Directive> cls, @Nullable ArtifactId artifactId)
    throws InstantiationException, IllegalAccessException {
    return new DirectiveInfo(DirectiveScope.USER, cls, artifactId);
  }

  /**
   * Initializes the class <code>DirectiveInfo</code>.
   *
   * @param scope of the directive.
   * @param directive a class of type directive.
   */
  private DirectiveInfo(DirectiveScope scope, Class<? extends Directive> directive, @Nullable ArtifactId artifactId) {
    this.directive = directive;
    this.directiveClass = new DirectiveClass(directive.getAnnotation(Name.class).value(),
                                             directive.getName(), scope, artifactId);

    Description desc = directive.getAnnotation(Description.class);
    if (desc == null) {
      this.description = "No description specified for directive class '" + directive.getSimpleName() + "'";
    } else {
      this.description = desc.value();
    }

    this.deprecated = directive.isAnnotationPresent(Deprecated.class);

    Categories category = directive.getAnnotation(Categories.class);
    if (category == null) {
      categories = new String[] { "default" };
    } else {
      categories = category.categories();
    }
  }

  /**
   * @return a {@link DirectiveClass} which contains the class information of this directive.
   */
  public DirectiveClass getDirectiveClass() {
    return directiveClass;
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
  public DirectiveScope scope() {
    return directiveClass.getScope();
  }

  /**
   * @return a <code>String</code> type specifying the name of the directive.
   */
  public String name() {
    return directiveClass.getName();
  }

  /**
   * @return a <code>String</code> type containing the usage information of the directive.
   */
  public String usage() {
    if (usage != null) {
      return usage;
    }
    UsageDefinition definition = definition();
    if (definition != null) {
      usage = definition.toString();
    } else {
      usage = "No definition available for directive '" + directive + "'";
    }
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
    if (definitionLoaded) {
      return definition;
    }

    synchronized (this) {
      if (definitionLoaded) {
        return definition;
      }
      try {
        definition = instance().define();
      } catch (IllegalAccessException | InstantiationException e) {
        throw new IllegalStateException(e);
      }
      definitionLoaded = true;
    }
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
    return directive.newInstance();
  }
}
