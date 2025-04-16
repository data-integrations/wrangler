/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.expression;

/**
 * Class description here.
 */
public final class ELResult {
  private final Object value;

  public ELResult(Object value) {
    this.value = value;
  }

  public Object getObject() {
    return value;
  }

  public Boolean getBoolean() {
    if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    return null;
  }

  public Integer getInteger() {
    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Long) {
      return ((Long) value).intValue();
    } else if (value instanceof Short) {
      return ((Short) value).intValue();
    } else if (value instanceof Double) {
      return ((Double) value).intValue();
    } else if (value instanceof Float) {
      return ((Float) value).intValue();
    }
    return null;
  }
}
