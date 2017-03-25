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

package co.cask.wrangler.service.request.v1;

import co.cask.wrangler.service.request.Request;

/**
 * Created by nitin on 3/24/17.
 */
public final class RequestV1 implements Request {
  private int version;
  private Workspace workspace;
  private Recipe recipe;
  private Sampling sampling;

  public RequestV1(int version, Workspace workspace, Recipe recipe, Sampling sampling) {
    this.version = version;
    this.workspace = workspace;
    this.recipe = recipe;
    this.sampling = sampling;
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public Workspace getWorkspace() {
    return workspace;
  }

  @Override
  public Sampling getSampling() {
    return sampling;
  }

  @Override
  public Recipe getRecipe() {
    return recipe;
  }
}
