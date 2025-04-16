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

import io.cdap.wrangler.api.annotations.Public;

/**
 * This class <code>Optional</code> is a helper class used in specifying
 * whether a argument for the directive is optional or not.
 *
 * <p>This class is used when you are defining the usage for a directive
 * argument. Following is an example : </p>
 *
 * <code>
 *   UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
 *   builder.define("regex", TokenType.TEXT, Optional.TRUE);
 *   return builder.build();
 * </code>
 *
 * <p>By default, the option is <code>FALSE</code></p>
 */
@Public
public final class Optional {
  /**
   * When an argument is optional, <code>TRUE</code> is specified.
   */
  public static final boolean TRUE = true;

  /**
   * When an argument is non optional, <code>FALSE</code> is specified.
   * The default behavior is false.
   */
  public static final boolean FALSE = false;
}
