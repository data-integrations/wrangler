/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.api;

import co.cask.wrangler.api.annotations.PublicEvolving;

import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A specification for how {@link RecipePipeline} will process.
 */
@PublicEvolving
public interface ParseDirectives extends Serializable {
  // Column definition for the start of processing.
  String STARTING_COLUMN = "__col";

  /**
   * Generates a configured set of {@link Directive} to be executed.
   *
   * @return List of {@link Directive}.
   */
  List<Directive> parse() throws DirectiveParseException;

  /**
   * Initialises the directive with a {@link DirectiveContext}.
   *
   * @param context, instance of context object or null.
   */
  @Nullable
  void initialize(DirectiveContext context);
}
