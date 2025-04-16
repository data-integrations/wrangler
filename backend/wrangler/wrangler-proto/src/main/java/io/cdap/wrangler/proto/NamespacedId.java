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

package io.cdap.wrangler.proto;

import java.util.Objects;

/**
 * A unique identifier for an entity within a namespace.
 * The 'id' field is not globally unique, but is unique within the context.
 * It is named 'id' for backward compatibility reasons.
 */
public class NamespacedId {
  protected final Namespace namespace;
  protected final String id;

  protected NamespacedId(NamespacedId other) {
    this(other.namespace, other.id);
  }

  public NamespacedId(Namespace namespace, String id) {
    this.namespace = namespace;
    this.id = id;
  }

  public Namespace getNamespace() {
    return namespace;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamespacedId that = (NamespacedId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, id);
  }
}
