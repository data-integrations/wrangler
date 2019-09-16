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
package io.cdap.wrangler.proto.datamodel;

import javax.annotation.Nullable;

/**
 * Meta information when listing all data models.
 * @param <T> the standard meta data type defined for the data model
 */
public class DataModelInfo<T> {
  private final String id;
  private final T standard;
  private final String version;

  public DataModelInfo(String id, T standard, String version) {
    this.id = id;
    this.standard = standard;
    this.version = version;
  }

  @Nullable
  public String getId() {
    return id;
  }

  @Nullable
  public T getStandard() {
    return standard;
  }

  @Nullable
  public String getVersion() {
    return version;
  }
}
