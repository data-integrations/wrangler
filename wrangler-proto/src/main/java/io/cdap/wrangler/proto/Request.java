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

package io.cdap.wrangler.proto;

import com.google.gson.JsonObject;

/**
 * Dataprep request body and it's structure.
 */
public interface Request {
  /**
   * @return Version of specification of request.
   */
  int getVersion();

  /**
   * @return Workspace specification associated with the request.
   */
  Workspace getWorkspace();

  /**
   * @return Sampling specification associated with the request.
   */
  Sampling getSampling();

  /**
   * @return Recipe specification associated with the request.
   */
  Recipe getRecipe();

  /**
   * @return Properties associated with the request.
   */
  JsonObject getProperties();
}
