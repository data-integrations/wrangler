/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.proto;

import com.google.gson.JsonObject;

/**
 * Specifies the V1 version of the {@link Request} object.
 */
public final class RequestV1 implements Request {
  // Version of request.
  private int version;

  // Workspace information associated with request.
  private Workspace workspace;

  // TestRecipe information associated with request.
  private Recipe recipe;

  // Sampling information associated with request.
  private Sampling sampling;

  // Additional properties that is of type json.
  private JsonObject properties;

  public RequestV1(int version, Workspace workspace, Recipe recipe, Sampling sampling,
                   JsonObject properties) {
    this.version = version;
    this.workspace = workspace;
    this.recipe = recipe;
    this.sampling = sampling;
    this.properties = properties;
  }

  /**
   * @return Version number of request specification.
   */
  @Override
  public int getVersion() {
    return version;
  }

  /**
   * @return {@link Workspace} information associated with this request.
   */
  @Override
  public Workspace getWorkspace() {
    return workspace;
  }

  /**
   * @return {@link Sampling} information associated with this request.
   */
  @Override
  public Sampling getSampling() {
    return sampling;
  }

  /**
   * @return {@link Recipe} information associated with this request.
   */
  @Override
  public Recipe getRecipe() {
    return recipe;
  }

  /**
   * @return Properties associated with the execution request.
   */
  @Override
  public JsonObject getProperties() {
    return properties;
  }
}
