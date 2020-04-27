/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import io.cdap.wrangler.api.parser.ColumnName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * The <code>Mutation</code> class represents a collection of relations which can be used to generate lineage for
 * a directive. It uses a UUID so that the name will be unique.
 *
 * <p>As this class is immutable, the constructor requires all the member variables to be presented
 * for an instance of this object to be created.</p>
 *
 * The class has methods to retrieve the <code>readable</code> text describing the transformation, and
 * <code>relations</code> to retrieve all the associations of source to targets
 *
 * @see Lineage
 * @see Relation
 * @see Many
 */
@Beta
public final class Mutation implements Serializable {
  private static final long serialVersionUID = 1243542667080258334L;
  private final String readable;
  private final List<Relation> relations;

  private Mutation() {
    this("", Collections.emptyList());
  }

  private Mutation(String readable, List<Relation> relations) {
    this.readable = readable;
    this.relations = Collections.unmodifiableList(new ArrayList<>(relations));
  }

  /**
   * @return a readable {@link String} version of the transformation to be included in lineage.
   */
  public String readable() {
    return readable;
  }

  /**
   * @return a {@link List} of {@link Relation} associated with the transformation.
   */
  public List<Relation> relations() {
    return relations;
  }

  /**
   * @return a instance of {@link Mutation.Builder}
   */
  public static Mutation.Builder builder() {
    return new Mutation.Builder();
  }

  /**
   * Builder to create Mutation.
   */
  public static class Builder {
    private final List<Relation> relations;
    private String description;

    /**
     * A builder constructor.
     */
    public Builder() {
      this.relations = new ArrayList<>();
    }

    /**
     * An easy way to created a formatted string of transformation.
     *
     * @param format a format string.
     * @param args Arguments referenced by the format specifiers in the format string.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder readable(String format, Object ... args) {
      this.description = String.format(format, args);
      return this;
    }

    /**
     * A variation to specify the readable string for transformation.
     *
     * @param format a format string.
     * @param args Arguments referenced by the format specifiers in the format string.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder readable(String format, List<Object> args) {
      readable(format, args.toArray());
      return this;
    }

    /**
     * Specifies a relation that need to be dropped.
     *
     * @param sources a list of sources to be dropped from lineage.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder drop(Many sources) {
      relations.add(new Relation(uuid(), sources.columns(),
                                 Collections.emptyList(), Relation.Type.DROP));
      return this;
    }

    /**
     * Specifies a relation that has no cause but effect.
     *
     * @param targets a list of targets.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder create(Many targets) {
      relations.add(new Relation(uuid(), Collections.emptyList(),
                                 targets.columns(), Relation.Type.CREATE));
      return this;
    }

    /**
     * A relation that has association with all the input fields.
     * This method is used usually during set columns scenarios.
     *
     * @param targets a list of targets.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder generate(Many targets) {
      relations.add(new Relation(uuid(), Collections.emptyList(),
                                 targets.columns(), Relation.Type.GENERATE));
      return this;
    }

    /**
     * A relation that has association with all in the output field.
     * This method is used usually during parse scenarios.
     *
     * @param sources list of sources to be associated with all output.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder all(Many sources) {
      relations.add(new Relation(uuid(), sources.columns(),
                                 Collections.emptyList(), Relation.Type.ALL));
      return this;
    }

    /**
     * A relation that has association with a set of targets + all in the output field.
     * This method is used usually during parse scenarios, where the directive knows some part of the relation,
     * but unclear about the overall outputs it will generate
     * For example, csv parser will parse field body -> a,b,c, and still preserve body.
     *
     * @param sources list of sources to be associated with targets and all output fields.
     * @param targets list of targets to be associated with the sources.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder all(Many sources, Many targets) {
      relations.add(new Relation(sources.columns(), targets.columns(), Relation.Type.ALL));
      return this;
    }

    /**
     * A standard relation between source of {@link ColumnName} to target of {@link ColumnName}.
     *
     * @param source a instance of {@link ColumnName} source.
     * @param target a instance of {@link ColumnName} target.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder relation(ColumnName source, ColumnName target) {
      relation(source.value(), target.value());
      return this;
    }

    /**
     * A relation that's conditional, depending on whether source and target are same.
     *
     * @param source a instance of {@link String} source.
     * @param target a instance of {@link String} target.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder conditional(String source, String target) {
      relation(source, source.equals(target) ? Many.of(target) : Many.of(source, target));
      return this;
    }

    /**
     * A standard one-to-one relation.
     *
     * @param source a source column.
     * @param target a target column.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder relation(String source, String target) {
      relation(source, Many.of(target));
      return this;
    }

    /**
     * A standard one-to-one relation.
     *
     * @param source a source column of type {@link ColumnName}.
     * @param targets a target column of type {@link ColumnName}.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder relation(ColumnName source, Many targets) {
      relation(source.value(), targets);
      return this;
    }

    /**
     * A relation specifying one-to-many.
     *
     * @param source a source column of type {@link String}.
     * @param targets {@link Many} target columns to be associated with source.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder relation(String source, Many targets) {
      relation(Many.of(source), targets);
      return this;
    }

    /**
     * Specifies a relation that is many-to-one.
     *
     * @param sources {@link Many} source columns to be associated with target.
     * @param target a target column of type {@link String}.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder relation(Many sources, String target) {
      relation(sources, Many.of(target));
      return this;
    }

    /**
     * Specifies {@link Many} to {@link Many} relation.
     *
     * @param sources {@link Many} source columns to be associated with {@link Many} targets.
     * @param targets {@link Many} target columns to be associated with {@link Many} sources.
     * @return a instance of {@link Mutation.Builder}
     */
    public Mutation.Builder relation(Many sources, Many targets) {
      relations.add(new Relation(uuid(), sources.columns(), targets.columns()));
      return this;
    }

    /**
     * Specifies a many-to-one relation.
     *
     * @param sources {@link Many} source columns to be associated with target.
     * @param target a instance of {@link ColumnName} target.
     * @return a instance of {@link Mutation.Builder}.
     */
    public Mutation.Builder relation(Many sources, ColumnName target) {
      relation(sources, target.value());
      return this;
    }

    private String uuid() {
      return UUID.randomUUID().toString();
    }

    public Mutation build() {
      return new Mutation(description, relations);
    }
  }
}
