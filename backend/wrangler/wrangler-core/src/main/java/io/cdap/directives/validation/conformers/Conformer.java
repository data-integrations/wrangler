/*
 *  Copyright © 2019 Cask Data, Inc.
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

package io.cdap.directives.validation.conformers;

import io.cdap.directives.validation.ConformanceIssue;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

/**
 * Conformer represents a type that can check whether some object instance of type T follows a set of restrictions (a
 * schema) enforced by this conformer. For example, a Conformer&lt;JsonObject&gt; can check if a specific JSON object
 * validates against a specific JSON Schema.
 *
 * @param <T> The type of value this conformer checks.
 */
public interface Conformer<T> {

  /**
   * Initialize should setup this instance to check conformance against a specific set of requirements, for example it
   * can load a specific JSON Schema into memory.
   */
  void initialize() throws IOException;

  /**
   * Validate the given value against the loaded schema. It is up to the implementation to decide whether to return the
   * first occurring error or all at once.
   *
   * @param value the value to validate
   * @return the deviations/failures (if any; otherwise an empty list) that the value ran into when validating
   */
  List<ConformanceIssue> checkConformance(T value);

  /**
   * Factory interface for Conformers.
   *
   * @param <T> The type of value the conformer (that this factory produces) checks.
   */
  interface Factory<T> {

    /**
     * Sets the stream containing schema data for the Conformer.
     */
    Factory<T> setSchemaStreamSupplier(Supplier<InputStream> schemaStream);

    /**
     * Instantiates a new Conformer.
     */
    Conformer<T> build();
  }
}
