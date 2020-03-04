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

package io.cdap.wrangler.api.lineage;

import io.cdap.cdap.api.annotation.Beta;

import java.io.Serializable;
import java.util.List;

/**
 * The <code>Relation</code> class represents a relation between a list of sources to a list of targets.
 * The relation can have 4 different types and can be many-to-many, many-to-one, one-to-many or one-to-one.
 *
 * <p>A relation can be of four different types namely:</p>
 * <ul>
 *   <li>
 *     <code>DROP type</code> - This type represents relation with a non-existential destination - one-to-none
 *   type. This type is used when the column wouldn't be present after the transformation is performed.
 *   </li>
 *   <li>
 *     <code>ALL</code> - This type represents a relation with many, one-to-many or many-to-many relation. This
 *     type is generally used when the transformation is unclear about what columns would be generated.
 *   </li>
 *   <li>
 *     <code>CREATE</code> - This type represents a relation in which a transformation creates a target without
 *     having any source.
 *   </li>
 *   <li>
 *     <code>GENERATE</code> - This type represents a relation with many, one-to-many or many-to-many relation. This
 *     type is generaly used when the transformation is unclear about the input columns
 *   </li>
 *   <li>
 *     <code>STANDARD</code> - This type represents all standard relations like many-to-many, many-to-one and
 *     one-to-many.
 *   </li>
 * </ul>
 *
 * @see Lineage
 * @see Many
 * @see Mutation
 */
@Beta
public final class Relation implements Serializable {
  private static final long serialVersionUID = 789984476035584877L;

  /**
   * Specifies the type of relations that can exist between source and target.
   */
  public enum Type {
    DROP,
    ALL,
    CREATE,
    GENERATE,
    STANDARD,
  }

  private final String id;
  private final List<String> sources;
  private final List<String> targets;
  private final Type type;

  /**
   * A constructor to create a relation with a unique id and associated source and target columns.
   * This method defaults the relation type to STANDARD
   *
   * @param id of the relation.
   * @param sources a {@link List} of sources columns.
   * @param targets a {@link List} of target columns.
   * @deprecated id is no longer needed to construct the relation, wrangler itself will generate the id
   */
  @Deprecated
  Relation(String id, List<String> sources, List<String> targets) {
    this(id, sources, targets, Type.STANDARD);
  }

  /**
   * A constructor to create a relation with a specified {@link Relation.Type} specified.
   *
   * @param id of the relation.
   * @param sources a {@link List} of sources columns.
   * @param targets a {@link List} of target columns.
   * @param type of relation.
   * @deprecated id is no longer needed to construct the relation, wrangler itself will generate the id
   */
  @Deprecated
  Relation(String id, List<String> sources, List<String> targets, Type type) {
    this.id = id;
    this.sources = sources;
    this.targets = targets;
    this.type = type;
  }

  /**
   * A constructor to create a relation with a unique id and associated source and target columns.
   * This method defaults the relation type to STANDARD
   *
   * @param sources a {@link List} of sources columns.
   * @param targets a {@link List} of target columns.
   */
  Relation(List<String> sources, List<String> targets) {
    this(null, sources, targets);
  }

  /**
   * A constructor to create a relation with a specified {@link Relation.Type} specified.
   *
   * @param sources a {@link List} of sources columns.
   * @param targets a {@link List} of target columns.
   * @param type of relation.
   */
  Relation(List<String> sources, List<String> targets, Type type) {
    this(null, sources, targets, type);
  }

  /**
   * @return a {@link String} representation of an id for uniquely identifying the relation.
   * @deprecated this id is no longer needed to construct the relation, the wrangler transform will generate id for
   * the relation
   */
  @Deprecated
  public String id() {
    return id;
  }

  /**
   * @return a {@link List} of source columns.
   */
  public List<String> getSources() {
    return sources;
  }

  /**
   * @return a {@link Relation.Type} this relation represents.
   */
  public Type getType() {
    return type;
  }

  /**
   * @return a {@link List} of target columns.
   */
  public List<String> getTargets() {
    return targets;
  }
}
