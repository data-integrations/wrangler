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

import java.util.List;

/**
 * Created by nitin on 3/24/17.
 */
public class Recipe {
  private List<String> directives;
  private Boolean save;
  private String name;

  public Recipe(List<String> directives, Boolean save, String name) {
    this.directives = directives;
    this.save = save;
    this.name = name;
  }

  public List<String> getDirectives() {
    return directives;
  }

  public Boolean getSave() {
    return save;
  }

  public String getName() {
    return name;
  }
}
