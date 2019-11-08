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

package io.cdap.directives.column;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;

/**
 * A Wrangler step for converting data type of a column
 * Accepted types are: int, short, long, double, float, string, boolean and bytes
 */
@Plugin(type = "directives")
@Name(SetType.NAME)
@Categories(categories = {"column"})
@Description("Converting data type of a column.")
public final class SetType implements Directive {
  public static final String NAME = "set-type";
  private String col;
  private String type;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("type", TokenType.IDENTIFIER);
    return builder.build();
  }

  @Override
  public List<FieldTransformOperation> getFieldOperations(StageContext context) {
    return Collections.singletonList(new FieldTransformOperation(String.format("Set type for column %s", col),
                                                                 String.format("Set type for column %s with" +
                                                                                 " value %s", col, type),
                                                                 Collections.singletonList(col), col));
  }

  @Override
  public void initialize(Arguments args) {
    col = ((ColumnName) args.value("column")).value();
    type = ((Identifier) args.value("type")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(col);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object == null || (object instanceof String && ((String) object).trim().isEmpty())) {
          continue;
        }
        try {
          row.setValue(idx, convertType(type, object));
        } catch (DirectiveExecutionException e) {
          throw e;
        } catch (Exception e) {
          throw new DirectiveExecutionException(
            NAME, String.format("Column '%s' cannot be converted to a '%s'.", col, type), e);
        }
      }
    }
    return rows;
  }

  private Object convertType(String toType, Object object) throws Exception {
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
        throw new DirectiveExecutionException(
          NAME, String.format(
            "Column '%s' is of unsupported type '%s'. Supported types are: " +
              "int, short, long, double, boolean, string, bytes", col, toType));
    }
  }
}
