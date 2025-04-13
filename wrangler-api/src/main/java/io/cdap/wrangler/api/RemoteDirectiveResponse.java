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

package io.cdap.wrangler.api;

import java.util.List;

/**
 * Response object containing transformed rows from a remotely executed directive.
 */
public class RemoteDirectiveResponse {
  /** The transformed rows. */
  private final List<Row> rows;

  /** The schema for transformed data. */
  private final String outputSchema;

  /**
   * Creates a new response.
   *
   * @param rows The transformed rows
   * @param outputSchema The schema for transformed data
   */
  public RemoteDirectiveResponse(final List<Row> rows, final String outputSchema) {
    this.rows = rows;
    this.outputSchema = outputSchema;
  }

  /**
   * Gets the transformed rows.
   *
   * @return List of transformed rows
   */
  public final List<Row> getRows() {
    return rows;
  }

  /**
   * Gets the output schema.
   *
   * @return Schema for transformed data
   */
  public final String getOutputSchema() {
    return outputSchema;
  }
}
