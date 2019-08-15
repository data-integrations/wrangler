/*
 *  Copyright © 2019 Google Inc.
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

import io.cdap.directives.validation.ConformanceException;
import io.cdap.wrangler.validator.Validator;
import io.cdap.wrangler.validator.ValidatorException;
import java.util.UUID;

public interface Conformer<T> extends Validator<T> {
  void checkConformance(T value, UUID id) throws ConformanceException;

  @Override
  default void validate(T value) throws ValidatorException {
    try {
      checkConformance(value, null);
    } catch (ConformanceException e) {
      throw new ValidatorException(e.getMessage());
    }
  }
}
