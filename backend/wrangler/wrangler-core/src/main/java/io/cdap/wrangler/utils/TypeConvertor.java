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

package io.cdap.wrangler.utils;

import io.cdap.cdap.api.common.Bytes;

/**
 * Provides various java type conversions.
 */
public final class TypeConvertor {

  /**
   * Converts a java type to String.
   *
   * @param object of any type.
   * @return object converted to type string.
   */
  public static String toString(Object object) throws IllegalArgumentException {
    if (object == null) {
      return null;
    }
    if (object instanceof String) {
      return (String) object;
    } else if (object instanceof Integer) {
      return Integer.toString((Integer) object);
    } else if (object instanceof Short) {
      return Short.toString((Short) object);
    } else if (object instanceof Long) {
      return Long.toString((Long) object);
    } else if (object instanceof Float) {
      return Float.toString((Float) object);
    } else if (object instanceof Double) {
      return Double.toString((Double) object);
    } else if (object instanceof byte[]) {
      return Bytes.toString((byte[]) object);
    } else if (object instanceof Character) {
      return Character.toString((Character) object);
    }

    throw new IllegalArgumentException(
      String.format("Cannot convert type '%s' to string", object.getClass().getSimpleName())
    );
  }
}
