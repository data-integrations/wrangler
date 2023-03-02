/*
 * Copyright © 2020 Cask Data, Inc.
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

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Utility class that converts a {@link Row} column into another column.
 */
public final class ColumnConverter {

  private ColumnConverter() {
  }

  /**
   * Renames a column. The renamed column must not exist in the row. The comparision is case insensitive.
   *
   * @param row source record to be modified.
   * @param column name of the column within source record.
   * @param toName the target name of the column.
   * @throws DirectiveExecutionException when a column matching the target name already exists
   */
  public static void rename(String directiveName, Row row, String column, String toName)
    throws DirectiveExecutionException {
    int idx = row.find(column);
    int existingColumn = row.find(toName);
    if (idx == -1) {
      return;
    }

    // if the idx are the same, this means the renamed column is same with the original column except the casing
    if (existingColumn == -1 || idx == existingColumn) {
      row.setColumn(idx, toName);
    } else {
      throw new DirectiveExecutionException(
        directiveName, String.format("Column '%s' already exists. Apply the 'drop %s' directive before " +
                                       "renaming '%s' to '%s'.",
                                     toName, toName, column, toName));
    }
  }

  /**
   * Converts the column type into another type. Only target types int, short, long, double, boolean, string, and bytes
   * are supported.
   *
   * @param row source record to be modified.
   * @param column name of the column within source record.
   * @param toType the target type of the column.
   * @throws DirectiveExecutionException when an unsupported type is specified or the column can not be converted.
   */
  public static void convertType(String directiveName, Row row, String column, String toType,
                                 Integer scale, RoundingMode roundingMode)
    throws DirectiveExecutionException {
    int idx = row.find(column);
    if (idx != -1) {
      Object object = row.getValue(idx);
      if (object == null || (object instanceof String && ((String) object).trim().isEmpty())) {
        return;
      }
      try {
        Object converted = ColumnConverter.convertType(column, toType, object);
        if (toType.equalsIgnoreCase("DECIMAL")) {
          row.setValue(idx, setDecimalScale((BigDecimal) converted, scale, roundingMode));
        } else {
          row.setValue(idx, converted);
        }
      } catch (DirectiveExecutionException e) {
        throw e;
      } catch (Exception e) {
        throw new DirectiveExecutionException(
          directiveName, String.format("Column '%s' cannot be converted to a '%s'.", column, toType), e);
      }
    }
  }

  private static Object convertType(String col, String toType, Object object)
    throws Exception {
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
        } else if (object instanceof BigDecimal) {
          return ((BigDecimal) object).intValue();
        } else if (object instanceof byte[]) {
          return Bytes.toInt((byte[]) object);
        } else {
          break;
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
        } else if (object instanceof BigDecimal) {
          return ((BigDecimal) object).shortValue();
        } else if (object instanceof byte[]) {
          return Bytes.toShort((byte[]) object);
        } else {
          break;
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
        } else if (object instanceof BigDecimal) {
          return ((BigDecimal) object).longValue();
        } else if (object instanceof byte[]) {
          return Bytes.toLong((byte[]) object);
        } else {
          break;
        }
      }

      case "BOOL":
      case "BOOLEAN": {
        if (object instanceof Boolean) {
          return object;
        } else if (object instanceof String) {
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
        } else if (object instanceof BigDecimal) {
          return ((BigDecimal) object).compareTo(BigDecimal.ZERO) > 0;
        } else if (object instanceof byte[]) {
          return Bytes.toBoolean((byte[]) object);
        } else {
          break;
        }
      }

      case "STRING": {
        if (object instanceof byte[]) {
          return Bytes.toString((byte[]) object);
        }
        return object.toString();
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
        } else if (object instanceof BigDecimal) {
          return ((BigDecimal) object).floatValue();
        } else if (object instanceof byte[]) {
          return Bytes.toFloat((byte[]) object);
        } else {
          break;
        }
      }

      case "DECIMAL": {
        if (object instanceof BigDecimal) {
          return object;
        } else if (object instanceof String) {
          return new BigDecimal((String) object);
        } else if (object instanceof Integer) {
          return BigDecimal.valueOf((Integer) object);
        } else if (object instanceof Short) {
          return BigDecimal.valueOf((Short) object);
        } else if (object instanceof Long) {
          return BigDecimal.valueOf((Long) object);
        } else if (object instanceof Float) {
          return BigDecimal.valueOf((Float) object);
        } else if (object instanceof Double) {
          return BigDecimal.valueOf((Double) object);
        } else if (object instanceof byte[]) {
          return Bytes.toBigDecimal((byte[]) object);
        } else {
          break;
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
        } else if (object instanceof BigDecimal) {
          return ((BigDecimal) object).doubleValue();
        } else {
          break;
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
        } else if (object instanceof BigDecimal) {
          return Bytes.toBytes((BigDecimal) object);
        } else if (object instanceof byte[]) {
          return object;
        } else {
          break;
        }
      }

      default:
        throw new DirectiveExecutionException(String.format(
          "Column '%s' is of unsupported type '%s'. Supported types are: " +
            "int, short, long, double, decimal, boolean, string, bytes", col, toType));
    }
    throw new DirectiveExecutionException(
        String.format("Column '%s' has value of type '%s' and cannot be converted to a '%s'.", col,
            object.getClass().getSimpleName(), toType));
  }

  private static BigDecimal setDecimalScale(BigDecimal decimal, Integer scale, RoundingMode roundingMode)
    throws DirectiveExecutionException {
    if (scale == null) {
      return decimal;
    }
    try {
      return decimal.setScale(scale, roundingMode);
    } catch (ArithmeticException e) {
      throw new DirectiveExecutionException(String.format(
        "Cannot set scale as '%s' for value '%s' when rounding-mode is '%s'", scale, decimal, roundingMode), e);
    }
  }
}
