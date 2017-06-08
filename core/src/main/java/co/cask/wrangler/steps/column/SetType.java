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

package co.cask.wrangler.steps.column;

import co.cask.cdap.api.common.Bytes;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.List;

/**
 * A Wrangler step for converting data type of a column
 * Accepted types are: int, double, string and boolean
 */
@Usage(
  directive = "set-type",
  usage = "set-type <column> <int|double|string|boolean>",
  description = "Converting data type of a column"
)
public class SetType extends AbstractStep {
  // Columns of the column to be upper-cased
  private String col;
  private String type;

  public SetType(int lineno, String detail, String col, String type) {
    super(lineno, detail);
    this.col = col;
    this.type = type;
  }

  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(col);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object == null) {
          continue;
        }
        try {
          record.setValue(idx, convertType(type, object));
        } catch (Exception e) {
          throw new StepException(
            String.format(toString() + ":" + e.getMessage())
          );
        }
      }
    }
    return records;
  }

  private Object convertType(String toType, Object object) throws Exception {
    toType = toType.toLowerCase();
    switch (toType) {
      case "INTEGER":
      case "I64":
      case "INT": {
        if(object instanceof String) {
          return Integer.parseInt((String) object);
        } else if (object instanceof Short) {
          return ((Short) object).intValue();
        } else if (object instanceof Float) {
          return ((Float) object).intValue();
        } else if (object instanceof Double) {
          return ((Double) object).intValue();
        } else if (object instanceof Number) {
          return ((Double) object).intValue();
        } else if (object instanceof Integer) {
          return object;
        } else if (object instanceof Long) {
          return ((Long) object).intValue();
        } else if (object instanceof byte[]) {
          return Bytes.toInt((byte[]) object);
        }
      }
      break;

      case "I32":
      case "SHORT": {
        if(object instanceof String) {
          return Short.parseShort((String) object);
        } else if (object instanceof Short) {
          return object;
        } else if (object instanceof Float) {
          return ((Float) object).shortValue();
        } else if (object instanceof Double) {
          return ((Double) object).shortValue();
        } else if (object instanceof Number) {
          return ((Double) object).shortValue();
        } else if (object instanceof Integer) {
          return ((Integer) object).shortValue();
        } else if (object instanceof Long) {
          return ((Long) object).shortValue();
        } else if (object instanceof byte[]) {
          return Bytes.toShort((byte[]) object);
        }
      }
      break;

      case "LONG": {
        if(object instanceof String) {
          return Long.parseLong((String) object);
        } else if (object instanceof Short) {
          return ((Short) object).longValue();
        } else if (object instanceof Float) {
          return ((Float) object).longValue();
        } else if (object instanceof Double) {
          return ((Double) object).longValue();
        } else if (object instanceof Number) {
          return ((Double) object).longValue();
        } else if (object instanceof Integer) {
          return ((Integer) object).longValue();
        } else if (object instanceof Long) {
          return object;
        } else if (object instanceof byte[]) {
          return Bytes.toLong((byte[]) object);
        }
      }
      break;

      case "BOOL":
      case "BOOLEAN": {
        if(object instanceof String) {
          return Boolean.parseBoolean((String) object);
        } else if (object instanceof Short) {
          return ((Short) object) > 0 ? true : false;
        } else if (object instanceof Float) {
          return ((Float) object) > 0 ? true : false;
        } else if (object instanceof Double) {
          return ((Double) object)  > 0 ? true : false;
        } else if (object instanceof Number) {
          return ((Double) object)  > 0 ? true : false;
        } else if (object instanceof Integer) {
          return ((Integer) object)  > 0 ? true : false;
        } else if (object instanceof Long) {
          return ((Long) object)  > 0 ? true : false;
        } else if (object instanceof byte[]) {
          return Bytes.toBoolean((byte[]) object);
        }
      }
      break;

      case "STRING": {
        if(object instanceof String) {
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
        }
      }
      break;

      case "FLOAT": {
        if(object instanceof String) {
          return Float.parseFloat((String) object);
        } else if (object instanceof Short) {
          return ((Short) object).floatValue();
        } else if (object instanceof Float) {
          return object;
        } else if (object instanceof Double) {
          return ((Double) object).floatValue();
        } else if (object instanceof Number) {
          return ((Double) object).floatValue();
        } else if (object instanceof Integer) {
          return ((Integer) object).floatValue();
        } else if (object instanceof Long) {
          return ((Long) object).longValue();
        } else if (object instanceof byte[]) {
          return Bytes.toFloat((byte[]) object);
        }
      }
      break;

      case "DOUBLE": {
        if(object instanceof String) {
          return Double.parseDouble((String) object);
        } else if (object instanceof Short) {
          return ((Short) object).doubleValue();
        } else if (object instanceof Float) {
          return ((Float) object).doubleValue();
        } else if (object instanceof Double) {
          return object;
        } else if (object instanceof Number) {
          return ((Double) object).doubleValue();
        } else if (object instanceof Integer) {
          return ((Integer) object).doubleValue();
        } else if (object instanceof Long) {
          return ((Long) object).doubleValue();
        } else if (object instanceof byte[]) {
          return Bytes.toDouble((byte[]) object);
        }
      }
      break;

      case "BYTES": {
        if(object instanceof String) {
          return Bytes.toBytes((String) object);
        } else if (object instanceof Short) {
          return Bytes.toBytes((Short) object);
        } else if (object instanceof Float) {
          return Bytes.toBytes((Float) object);
        } else if (object instanceof Double) {
          return Bytes.toBytes((Double) object);
        } else if (object instanceof Number) {
          return Bytes.toBytes((Double) object);
        } else if (object instanceof Integer) {
          return Bytes.toBytes((Integer) object);
        } else if (object instanceof Long) {
          return Bytes.toBytes((Long) object);
        } else if (object instanceof byte[]) {
          return object;
        }
      }
      break;

    }

  }
}
