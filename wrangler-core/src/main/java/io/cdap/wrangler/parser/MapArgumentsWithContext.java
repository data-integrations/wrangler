/*
 *  Copyright © 2026 Cask Data, Inc.
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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.parser.UsageDefinition;

/**
 * A {@link MapArguments} that also holds a {@link DirectiveContext}.
 */
public class MapArgumentsWithContext extends MapArguments {
  private final DirectiveContext context;

  public MapArgumentsWithContext(UsageDefinition definition, TokenGroup group, DirectiveContext context)
    throws DirectiveParseException {
    super(definition, group);
    this.context = context;
  }

  public DirectiveContext getDirectiveContext() {
    return context;
  }
}
