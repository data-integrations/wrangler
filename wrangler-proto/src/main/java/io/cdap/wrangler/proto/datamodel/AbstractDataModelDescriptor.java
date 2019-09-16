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

import io.cdap.wrangler.proto.NamespacedId;

import java.util.Objects;

/**
 * Common functionality for information describing a data model.
 * @param <T> type of data model
 */
public abstract class AbstractDataModelDescriptor<T> {

  private final NamespacedId namespacedId;
  private final T dataModel;
  private final DataModelType dataModelType;

  public AbstractDataModelDescriptor(NamespacedId namespacedId, T dataModel, DataModelType dataModelType) {
    this.namespacedId = namespacedId;
    this.dataModel = dataModel;
    this.dataModelType = dataModelType;
  }

  /**
   * @return the id of the schema descriptor.
   */
  public NamespacedId getNamespacedId() {
    return namespacedId;
  }

  /**
   * @return the data model (e.g. JSONSchema) referenced by the descriptor.
   */
  public T getDataModel() {
    return dataModel;
  }

  /**
   * @return the {@DataModelType} referenced by the schema.
   */
  public DataModelType getDataModelType() {
    return dataModelType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractDataModelDescriptor<T> descriptor = (AbstractDataModelDescriptor<T>) o;
    return Objects.equals(namespacedId, descriptor.namespacedId) &&
        Objects.equals(dataModel, descriptor.dataModel);
  }
}
