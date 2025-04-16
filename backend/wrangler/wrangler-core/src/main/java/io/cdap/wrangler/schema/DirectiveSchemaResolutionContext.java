/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.wrangler.schema;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.SchemaResolutionContext;

/**
 * Context to pass information related to getting or generating the output schema of a {@link Directive}
 */
public class DirectiveSchemaResolutionContext implements SchemaResolutionContext {
  private final Schema inputSchema;
  public DirectiveSchemaResolutionContext(Schema inputSchema) {
    this.inputSchema = inputSchema;
  }

  @Override
  public Schema getInputSchema() {
    return inputSchema;
  }
}
