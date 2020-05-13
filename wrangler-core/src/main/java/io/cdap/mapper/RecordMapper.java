package io.cdap.mapper;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RecordMapper {

  public Map<String, Object> toMap(StructuredRecord record) {
    Schema schema = record.getSchema();
    schema = schema.isNullable() ? schema.getNonNullable() : schema;
    return decodeRecord(record, schema);
  }

  public Map<String, Object> toMap(Schema schema) {
    schema = schema.isNullable() ? schema.getNonNullable() : schema;
    return decodeRecord(schema);
  }

  public StructuredRecord fromMap(Map<String, Object> object, Schema schema) {
    schema = schema.isNullable() ? schema.getNonNullable() : schema;
    return encodeRecord(object, schema);
  }

  private void validate(String name, String type, Class<?> clazz, Object o) {
    if (! clazz.isAssignableFrom(o.getClass())) {
      throw new RuntimeException(
        String.format("Field '%s' of type '%s' is being assigned a value of type '%s'.", name,
                      type, o.getClass().getSimpleName().toLowerCase())
      );
    }
  }

  private StructuredRecord encodeRecord(Map<String, Object> object, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Schema.Type type = fieldSchema.getType();
      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      String name = field.getName();
      Object o = object.get(name);
      if (o == null) {
        builder.set(name, o);
        continue;
      }

      if (logicalType != null) {
        switch (logicalType) {
          case DECIMAL:
            validate(name, logicalType.name().toLowerCase(), BigDecimal.class, o);
            builder.set(name, ((BigDecimal)o).unscaledValue().toByteArray());
            break;

          case DATE:
            validate(name, logicalType.name().toLowerCase(), LocalDate.class, o);
            builder.set(name, o);
            break;

          case TIME_MICROS:
          case TIME_MILLIS:
            validate(name, logicalType.name().toLowerCase(), LocalTime.class, o);
            builder.set(name, o);
            break;

          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            validate(name, logicalType.name().toLowerCase(), ZonedDateTime.class, o);
            builder.set(name, o);
            break;
        }
        continue;
      }

      switch (type) {
        case STRING: {
          validate(name, type.name().toLowerCase(), String.class, o);
          builder.set(name, o);
          break;
        }

        case INT: {
          validate(name, type.name().toLowerCase(), Integer.class, o);
          builder.set(name, o);
          break;
        }

        case LONG:{
          validate(name, type.name().toLowerCase(), Long.class, o);
          builder.set(name, o);
          break;
        }

        case FLOAT:{
          validate(name, type.name().toLowerCase(), Float.class, o);
          builder.set(name, o);
          break;
        }

        case DOUBLE:{
          validate(name, type.name().toLowerCase(), Double.class, o);
          builder.set(name, o);
          break;
        }
        case BOOLEAN:{
          validate(name, type.name().toLowerCase(), Boolean.class, o);
          builder.set(name, o);
          break;
        }

        case BYTES:{
          validate(name, type.name().toLowerCase(), Byte.class, o);
          builder.set(name, o);
          break;
        }

        case NULL:{
          builder.set(name, o);
          break;
        }

        case RECORD:
          builder.set(name, encodeRecord((Map)object.get(name), fieldSchema));
          break;

        case ARRAY:
          builder.set(name, encodeArray((List)object.get(name), fieldSchema));
          break;
      }
    }
    return builder.build();
  }

  private Object encode(Object o, Schema schema) {
    Schema.Type type = schema.getType();

    switch (type) {
      case STRING:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case NULL:
        return o;

      case RECORD:
        return encodeRecord((Map) o, schema);

      case ARRAY:
        return encodeArray((List) o, schema);

      case UNION:
        return decode(schema.getNonNullable());
    }
    throw new RuntimeException("Bad type");
  }

  private Object encodeArray(List objects, Schema fieldSchema) {
    int size = objects.size();
    ArrayList<Object> list = new ArrayList<>(size);
    Schema componentSchema = fieldSchema.getComponentSchema();
    componentSchema = componentSchema.isNullable() ? componentSchema.getNonNullable() : componentSchema;
    for (int i = 0; i < size; ++i) {
      list.add(encode(objects.get(i), componentSchema));
    }
    return list;
  }


  private Map<String, Object> decodeRecord(StructuredRecord record, Schema schema) {
    Map<String, Object> objects = new HashMap<>();
    List<Schema.Field> fields = schema.getFields();
    for(Schema.Field field : fields) {
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Schema.Type type = fieldSchema.getType();
      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      String name = field.getName();

      if (logicalType != null) {
        switch (logicalType) {
          case DECIMAL:
            objects.put(name, record.getDecimal(name));
            break;

          case DATE:
            objects.put(name, record.getDate(name));
            break;

          case TIME_MICROS:
          case TIME_MILLIS:
            objects.put(name, record.getTime(name));
            break;

          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            objects.put(name, record.getTimestamp(name));
            break;
        }
        continue;
      }

      switch (type) {
        case STRING:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case BYTES:
        case NULL:
          objects.put(name, record.get(name));
          break;

        case RECORD:
          objects.put(name, decodeRecord(record.get(name), fieldSchema));
          break;

        case ARRAY:
          objects.put(name, decodeArray(record.get(name), fieldSchema));
          break;
      }
    }
    return objects;
  }

  private Object decode(Object o, Schema schema) {
    Schema.Type type = schema.getType();

    switch (type) {
      case STRING:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case NULL:
        return o;

      case RECORD:
        return decodeRecord((StructuredRecord) o, schema);

      case ARRAY:
        return decodeArray((List) o, schema);

      case UNION:
        return decode(o, schema.getNonNullable());
    }
    throw new RuntimeException("Bad type");
  }

  private List decodeArray(List o, Schema fieldSchema) {
    List<Object> list = new ArrayList<>(o.size());
    int size = o.size();
    for (int i = 0; i < size; ++i) {
      list.add(decode(o.get(i), fieldSchema.getComponentSchema()));
    }
    return list;
  }

  private Map<String, Object> decodeRecord(Schema schema) {
    Map<String, Object> objects = new HashMap<>();
    List<Schema.Field> fields = schema.getFields();
    for(Schema.Field field : fields) {
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Schema.Type type = fieldSchema.getType();
      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      String name = field.getName();

      if (logicalType != null) {
        switch (logicalType) {
          case DECIMAL:
            objects.put(name, null);
            break;

          case DATE:
            objects.put(name, null);
            break;

          case TIME_MICROS:
          case TIME_MILLIS:
            objects.put(name, null);
            break;

          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            objects.put(name, null);
            break;
        }
        continue;
      }

      switch (type) {
        case STRING:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case BYTES:
        case NULL:
          objects.put(name, null);
          break;

        case RECORD:
          objects.put(name, decodeRecord(fieldSchema));
          break;

        case ARRAY:
          objects.put(name, new ArrayList<>());
          break;
      }
    }
    return objects;
  }

  private Object decode(Schema schema) {
    Schema.Type type = schema.getType();

    switch (type) {
      case STRING:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case NULL:
        return null;

      case RECORD:
        return decodeRecord(schema);

      case ARRAY:
        return new ArrayList<>();
    }
    throw new RuntimeException("Bad type");
  }
}
