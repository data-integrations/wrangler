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

package io.cdap.wrangler.dataset.datamodel;

/**
 * POJO representing configuration information for a the {@DataModelStore}
 */
public class DataModelConfig {

  private boolean isDataModelsLoaded;

  public DataModelConfig(boolean isDataModelsLoaded) {
    this.isDataModelsLoaded = isDataModelsLoaded;
  }

  public boolean getIsDataModelsLoaded() {
    return this.isDataModelsLoaded;
  }
}
