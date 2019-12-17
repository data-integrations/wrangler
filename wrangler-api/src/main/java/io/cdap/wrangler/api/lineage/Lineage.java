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

/**
 * This class <code>Lineage</code> defines a mutation defined by the directive that gets capture as lineage.
 *
 * <p>Directives have to implement this class to inject their mutations for lineage to be constructed.</p>
 * <p>
 *   The method <code>lineage</code> is invoked separately in the <code>prepareRun</code> phase of the pipeline
 *   execution. Before the method <code>lineage</code> is invoked, the framework ensures that the receipe is
 *   parsed and initialize on each directive that is included is called. All the class variables of the directive
 *   are available to be used within the <code>lineage</code> method.
 * </p>
 *
 * <p>
 *   {@link Mutation} captures all the changes the directive is going to be applying of the data. It has
 *   two major methods:
 *
 *   <ul>
 *     <il>
 *       <code>readable</code> - This method is defined to provide the post transformation description of the mutation
 *     the directive is applying on data. Care should be taken to use the right tense as the lineage would be consumed
 *     by users after the transformation has been applied on the data. As best practise, it's highly recommended to use
 *     past-tense for describing the transformations. Additionally, the language of the description is not trying
 *     to provide complete details and configuration, but actually focusing on operations that directive has
 *     performed on data.
 *     </il>
 *     <li>
 *       <code>relation</code> - This methods defines the relations between various columns either being used
 *     as target or source for performing the data transformation. </li>
 *   </ul>
 * </p>
 *
 * Following are few examples of how the method can be implemented:
 *
 * <code>
 * @Override
 * public Mutation lineage() {
 *   return Mutation.builder()
 *    .readable("Looking up catalog using value in column '%s' and results written into column '%s', src, dest)
 *    .relation(src, Many.of(src, dest))
 *    .build();
 * }
 * </code>
 *
 * Another example:
 *
 * <code>
 * @Override
 * public Mutation lineage() {
 *   return Mutation.builder()
 *    .readable("Dropped columns %s", columns")
 *    .drop(Many.of(columns))
 *    .build();
 * }
 * </code>
 *
 * @see Mutation
 * @see Relation
 * @see Many
 */
@Beta
public interface Lineage {
  /**
   * Returns a Mutation that can be used to generate lineage.
   *
   * @return a instance of {@link Mutation} object used to generate lineage.
   */
  Mutation lineage();
}
