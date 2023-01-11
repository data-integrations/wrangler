/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.wrangler.proto.workspace.v2;

/**
 * Response returned when a {@link io.cdap.wrangler.api.RecipeException} occurs. Contains information about row index
 * in dataset and directive index in recipe that caused the error.
 * @param <T>
 */
public class RecipeExceptionResponse<T> extends ServiceResponse<T> {
  private final Integer rowIndex;
  private final Integer directiveIndex;
  public RecipeExceptionResponse(String message, Integer rowIndex, Integer directiveIndex) {
    super(message);
    this.rowIndex = rowIndex;
    this.directiveIndex = directiveIndex;
  }
}
