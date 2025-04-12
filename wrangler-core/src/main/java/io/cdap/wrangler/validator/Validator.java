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

package io.cdap.wrangler.validator;

import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * Interface for Validating 'T' properties.
 *
 * @param <T> type of object to validate
 */
@PublicEvolving
public interface Validator<T> {
  /**
   * Initializes the validator.
   * @throws Exception thrown when there are initialization error.
   */
  void initialize() throws Exception;

  /**
   * Validates the T properties.
   *
   * @param value to be validated.
   * @throws ValidatorException thrown when there are issues with validation.
   */
  void validate(T value) throws ValidatorException;
}
