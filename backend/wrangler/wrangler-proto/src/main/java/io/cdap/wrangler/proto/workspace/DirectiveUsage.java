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

package io.cdap.wrangler.proto.workspace;

import io.cdap.wrangler.api.parser.UsageDefinition;

/**
 * Information about how to use a directive
 */
public class DirectiveUsage {
  private final String directive;
  private final String usage;
  private final String description;
  private final boolean excluded;
  private final boolean alias;
  private final String scope;
  private final UsageDefinition arguments;
  private final String[] categories;

  public DirectiveUsage(String directive, String usage, String description, boolean excluded, boolean alias,
                        String scope, UsageDefinition arguments, String[] categories) {
    this.directive = directive;
    this.usage = usage;
    this.description = description;
    this.excluded = excluded;
    this.alias = alias;
    this.scope = scope;
    this.arguments = arguments;
    this.categories = categories;
  }
}
