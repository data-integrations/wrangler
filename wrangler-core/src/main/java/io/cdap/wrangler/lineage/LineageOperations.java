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

package io.cdap.wrangler.lineage;

import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * This class {@link LineageOperations} generates transformation operations used to generate lineage.
 */
public final class LineageOperations {
  private final Set<String> input;
  private final Set<String> output;
  private final List<Directive> directives;

  /**
   * A constructor for generating the transformation operations for lineage.
   *
   * @param input A input {@link Set} of columns.
   * @param output A output {@link Set} of columns.
   * @param directives A {@link List} of directives.
   */
  public LineageOperations(Set<String> input, Set<String> output, List<Directive> directives) {
    this.input = input;
    this.output = output;
    this.directives = directives;
  }

  /**
   * Generates the list of {@link FieldOperation} required for generating lineage.
   *
   * @return a {@link List} of {@link FieldOperation}
   */
  public List<FieldOperation> generate() {
    List<FieldOperation> operations = new ArrayList<>();
    Set<String> definedSources = new HashSet<>();
    for (Directive directive : directives) {
      if (directive instanceof Lineage) {
        Mutation mutation = ((Lineage) directive).lineage();
        String readable = mutation.readable();
        mutation.relations().forEach(
          relation -> {
            String id = relation.id();
            List<String> sources = relation.getSources();
            List<String> targets = relation.getTargets();
            switch (relation.getType()) {
              case ALL:
                sources = sources.isEmpty() ? new ArrayList<>(input) : sources;
                operations.add(
                  new FieldTransformOperation(
                    relation.id(),
                    readable,
                    sources,
                    new ArrayList<>(output))
                );
                break;

              case DROP:
                operations.add(
                  new FieldTransformOperation(
                    relation.id(),
                    readable,
                    sources
                  )
                );
                break;

              case CREATE:
                operations.add(
                  new FieldTransformOperation(
                    id,
                    readable,
                    Collections.emptyList(),
                    targets
                  )
                );
                break;

              case STANDARD:
                operations.add(
                  new FieldTransformOperation(
                    id,
                    readable,
                    sources,
                    targets
                  )
                );
            }
            definedSources.addAll(relation.getSources());
          }
        );
      }
    }

    // We iterate through all the input fields in the schema, check if there is corresponding
    // field in the output schema. If both exists, then a identity mapping transform is added
    // to the {@code FieldTransformationOperation} is added.
    input.removeAll(definedSources);
    Iterator<String> iterator = input.iterator();
    while (iterator.hasNext()) {
      String next = iterator.next();
      if (output.contains(next)) {
        FieldTransformOperation transformation =
          new FieldTransformOperation(
            UUID.randomUUID().toString(), // an id.
            String.format("Mapping column '%s' to column '%s'", next, next),
            Collections.singletonList(next),
            next
          );
        operations.add(transformation);
      }
    }

    return operations;
  }
}
