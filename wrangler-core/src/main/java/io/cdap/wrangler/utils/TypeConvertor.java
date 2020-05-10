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
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.Row;

/**
 * Provides various java type conversions.
 */
public final class TypeConvertor {

  /**
   * Converts the column type into another type. Only target types int, short, long, double, boolean, string, and bytes
   * are supported.
   *
   * @param row source record to be modified.
   * @param column name of the column within source record.
   * @param toType the target type of the column.
   * @throws DirectiveExecutionException when an unsupported type is specified or the column can not be converted.
   */
  public static void convertType(String name, Row row, String column, String toType)
    throws DirectiveExecutionException {
    int idx = row.find(column);
    if (idx != -1) {
      Object object = row.getValue(idx);
      if (object == null || (object instanceof String && ((String) object).trim().isEmpty())) {
        return;
      }
      try {
        row.setValue(idx, convertType(column, toType, object));
      } catch (DirectiveExecutionException e) {
        throw e;
      } catch (Exception e) {
        throw new DirectiveExecutionException(
          name, String.format("Column '%s' cannot be converted to a '%s'.", column, toType), e);
      }
    }
  }

  private static Object convertType(String col, String toType, Object object) throws Exception {
    toType = toType.toUpperCase();
    switch (toType) {
      case "INTEGER":
      case "I64":
      case "INT": {
        if (object instanceof String) {
          return Integer.parseInt((String) object);
        } else if (object instanceof Short) {
          return ((Short) object).intValue();
        } else if (object instanceof Float) {
          return ((Float) object).intValue();
        } else if (object instanceof Double) {
          return ((Double) object).intValue();
        } else if (object instanceof Integer) {
          return object;
        } else if (object instanceof Long) {
          return ((Long) object).intValue();
        } else if (object instanceof byte[]) {
          return Bytes.toInt((byte[]) object);
        } else {
          return object;
        }
      }

      case "I32":
      case "SHORT": {
        if (object instanceof String) {
          return Short.parseShort((String) object);
        } else if (object instanceof Short) {
          return object;
        } else if (object instanceof Float) {
          return ((Float) object).shortValue();
        } else if (object instanceof Double) {
          return ((Double) object).shortValue();
        } else if (object instanceof Integer) {
          return ((Integer) object).shortValue();
        } else if (object instanceof Long) {
          return ((Long) object).shortValue();
        } else if (object instanceof byte[]) {
          return Bytes.toShort((byte[]) object);
        } else {
          return object;
        }
      }

      case "LONG": {
        if (object instanceof String) {
          return Long.parseLong((String) object);
        } else if (object instanceof Short) {
          return ((Short) object).longValue();
        } else if (object instanceof Float) {
          return ((Float) object).longValue();
        } else if (object instanceof Double) {
          return ((Double) object).longValue();
        } else if (object instanceof Integer) {
          return ((Integer) object).longValue();
        } else if (object instanceof Long) {
          return object;
        } else if (object instanceof byte[]) {
          return Bytes.toLong((byte[]) object);
        } else {
          return object;
        }
      }

      case "BOOL":
      case "BOOLEAN": {
        if (object instanceof String) {
          return Boolean.parseBoolean((String) object);
        } else if (object instanceof Short) {
          return ((Short) object) > 0 ? true : false;
        } else if (object instanceof Float) {
          return ((Float) object) > 0 ? true : false;
        } else if (object instanceof Double) {
          return ((Double) object) > 0 ? true : false;
        } else if (object instanceof Integer) {
          return ((Integer) object) > 0 ? true : false;
        } else if (object instanceof Long) {
          return ((Long) object) > 0 ? true : false;
        } else if (object instanceof byte[]) {
          return Bytes.toBoolean((byte[]) object);
        } else {
          return object;
        }
      }

      case "STRING": {
        if (object instanceof String) {
          return object;
        } else if (object instanceof Short) {
          return Short.toString((Short) object);
        } else if (object instanceof Float) {
          return Float.toString((Float) object);
        } else if (object instanceof Double) {
          return Double.toString((Double) object);
        } else if (object instanceof Integer) {
          return Integer.toString((Integer) object);
        } else if (object instanceof Long) {
          return Long.toString((Long) object);
        } else if (object instanceof byte[]) {
          return Bytes.toString((byte[]) object);
        } else if (object instanceof Boolean) {
          return Boolean.toString((Boolean) object);
        } else {
          return object;
        }
      }

      case "FLOAT": {
        if (object instanceof String) {
          return Float.parseFloat((String) object);
        } else if (object instanceof Short) {
          return ((Short) object).floatValue();
        } else if (object instanceof Float) {
          return object;
        } else if (object instanceof Double) {
          return ((Double) object).floatValue();
        } else if (object instanceof Integer) {
          return ((Integer) object).floatValue();
        } else if (object instanceof Long) {
          return ((Long) object).floatValue();
        } else if (object instanceof byte[]) {
          return Bytes.toFloat((byte[]) object);
        } else {
          return object;
        }
      }

      case "DOUBLE": {
        if (object instanceof String) {
          return Double.parseDouble((String) object);
        } else if (object instanceof Short) {
          return ((Short) object).doubleValue();
        } else if (object instanceof Float) {
          return ((Float) object).doubleValue();
        } else if (object instanceof Double) {
          return object;
        } else if (object instanceof Integer) {
          return ((Integer) object).doubleValue();
        } else if (object instanceof Long) {
          return ((Long) object).doubleValue();
        } else if (object instanceof byte[]) {
          return Bytes.toDouble((byte[]) object);
        } else {
          return object;
        }
      }

      case "BYTES": {
        if (object instanceof String) {
          return Bytes.toBytes((String) object);
        } else if (object instanceof Short) {
          return Bytes.toBytes((Short) object);
        } else if (object instanceof Float) {
          return Bytes.toBytes((Float) object);
        } else if (object instanceof Double) {
          return Bytes.toBytes((Double) object);
        } else if (object instanceof Integer) {
          return Bytes.toBytes((Integer) object);
        } else if (object instanceof Long) {
          return Bytes.toBytes((Long) object);
        } else if (object instanceof byte[]) {
          return object;
        } else {
          return object;
        }
      }

      default:
        throw new DirectiveExecutionException(String.format(
          "Column '%s' is of unsupported type '%s'. Supported types are: " +
            "int, short, long, double, boolean, string, bytes", col, toType));
    }
  }

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
