package co.cask.wrangler.service;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.internal.SchemaUtilities;
import co.cask.wrangler.internal.TextDirectives;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests generation of schema.
 */
public class SchemaGenerationTest {

  @Test
  public void testExtensive() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "{\n" +
        "    \"name\" : {\n" +
        "        \"fname\" : \"Joltie\",\n" +
        "        \"lname\" : \"Root\",\n" +
        "        \"mname\" : null\n" +
        "    },\n" +
        "    \"coordinates\" : [\n" +
        "        12.56,\n" +
        "        45.789\n" +
        "    ],\n" +
        "\n" +
        "    \"moves\" : [\n" +
        "        { \"a\" : 1, \"b\" : \"X\", \"c\" : 2.8},\n" +
        "        { \"a\" : 2, \"b\" : \"Y\", \"c\" : 232342.8},\n" +
        "        { \"a\" : 3, \"b\" : \"Z\"}\n" +
        "    ],\n" +
        "    \"integer\" : 1,\n" +
        "    \"double\" : 2.8,\n" +
        "    \"float\" : 45.6,\n" +
        "    \"aliases\" : [\n" +
        "        \"root\",\n" +
        "        \"joltie\",\n" +
        "        \"bunny\"\n" +
        "    ]\n" +
        "}")
    );

    records = execute(directives, records);
    Schema schema = SchemaUtilities.recordToSchema("record", records);
    List<StructuredRecord> srecords = toStructuredRecord(records, schema);
    Assert.assertTrue(records.size() > 1);
  }

  private StructuredRecord toStructuredRecord(Record record, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      Object value = record.getValue(name);
      builder.set(name, decode(name, value, field.getSchema()));
    }
    return builder.build();
  }

  @SuppressWarnings("RedundantCast")
  private Object decodeSimpleType(String name, Object object, Schema schema) {
    Schema.Type type = schema.getType();
    switch (type) {
      case NULL:
        return null;
      // numbers come back as Numbers
      case INT:
        if (object instanceof Integer) {
          return (Integer) object;
        } else if (object instanceof String) {
          try {

          } catch (NumberFormatException e) {

          }
        }
        return ((Number) object).intValue();
      case LONG:
        return ((Number) object).longValue();
      case FLOAT:
        return ((Number) object).floatValue();
      case DOUBLE:
        // case so that if it's not really a double it will fail. This is possible for unions,
        // where we don't know what the actual type of the object should be.
        return ((Number) object).doubleValue();
      case BOOLEAN:
        return (Boolean) object;
      case STRING:
        return (String) object;
    }
    throw new RuntimeException("Unable decode object with schema " + schema);
  }

  private Object decode(String name, Object object, Schema schema) {
    Schema.Type type = schema.getType();

    switch (type) {
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
        return decodeSimpleType(name, object, schema);
      case ENUM:
        break;
      case ARRAY:
        return decodeArray(name, (List)object, schema.getComponentSchema());
      case MAP:
        Schema keySchema = schema.getMapSchema().getKey();
        Schema valSchema = schema.getMapSchema().getValue();
        // Should be fine to cast since schema tells us what it is.
        //noinspection unchecked
        return decodeMap(name, (Map<Object, Object>) object, keySchema, valSchema);
      case UNION:
        return decodeUnion(name, object, schema.getUnionSchemas());
    }
    throw new RuntimeException("Unable decode object with schema " + schema);
  }

  private Object decodeUnion(String name, Object object, List<Schema> schemas) {
    for (Schema schema : schemas) {
      try {
        return decode(name, object, schema);
      } catch (Exception e) {
        // could be ok, just move on and try the next schema
      }
    }
    throw new RuntimeException("Unable decode union with schema " + schemas);
  }

  private List<Object> decodeArray(String name, List nativeArray, Schema componentSchema) {
    List<Object> arr = Lists.newArrayListWithCapacity(nativeArray.size());
    for (Object arrObj : nativeArray) {
      arr.add(decode(name, arrObj, componentSchema));
    }
    return arr;
  }

  private Map<Object, Object> decodeMap(String name, Map<Object, Object> object, Schema keySchema, Schema valSchema) {
    Map<Object, Object> output = Maps.newHashMap();
    for (Map.Entry<Object, Object> entry : object.entrySet()) {
      output.put(decode(name, entry.getKey(), keySchema), decode(name, entry.getValue(), valSchema));
    }
    return output;
  }



  private List<StructuredRecord> toStructuredRecord(List<Record> records, Schema schema) {
    List<StructuredRecord> results = new ArrayList<>();
    for (Record record : records) {
      StructuredRecord r = toStructuredRecord(record, schema);
      results.add(r);
    }
    return results;
  }


//  private List<StructuredRecord> toStructuredRecord(List<Record> records, Schema schema) {
//    List<StructuredRecord> results = new ArrayList<>();
//    for (Record record : records) {
//      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
//      List<Schema.Field> fields = schema.getFields();
//      for (Schema.Field field : fields) {
//        String name = field.getName();
//        Object value = record.getValue(name);
//
//        if (value != null) {
//          if (value instanceof JSONArray) {
//            List<StructuredRecord> arrays = null;
//            try {
//              arrays = toStructuredRecord(name, (JSONArray) value, field.getSchema());
//            } catch (JSONException e) {
//              e.printStackTrace();
//            }
//            builder.set(name, arrays);
//          } else {
//            builder.set(name, value);
//          }
//        }
//      }
//      results.add(builder.build());
//    }
//    return results;
//  }

  private List<StructuredRecord> toStructuredRecord(String name, JSONArray value, Schema schema) throws JSONException {
    List<StructuredRecord> records = new ArrayList<>();
    Schema.Type type = schema.getType();
    if (schema.isNullable() && type == Schema.Type.UNION) {
      Schema s = schema.getNonNullable().getComponentSchema();
      type = s.getType();
    }

    if (type == Schema.Type.RECORD) {
      for (int i = 0; i < value.length(); ++i) {
        JSONObject object = value.getJSONObject(0);
        StructuredRecord.Builder builder = StructuredRecord.builder(schema.getNonNullable().getComponentSchema());
        for (Schema.Field field : schema.getNonNullable().getComponentSchema().getFields()) {
          Object v = object.get(field.getName());
          builder.set(field.getName(), v);
        }
        records.add(builder.build());
      }
    } else {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      if (type == Schema.Type.DOUBLE) {
        List<Double> values = new ArrayList<>();
        for (int i = 0; i < value.length(); ++i) {
          values.add(value.getDouble(i));
        }
      } else if(type == Schema.Type.LONG) {
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < value.length(); ++i) {
          values.add(value.getLong(i));
        }
      } else if (type == Schema.Type.STRING) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < value.length(); ++i) {
          values.add(value.getString(i));
        }
      }
    }
    return records;
  }

  public static List<Record> execute(List<Step> steps, List<Record> records) throws StepException {
    for (Step step : steps) {
      records = step.execute(records, null);
    }
    return records;
  }

  public static List<Record> execute(String[] directives, List<Record> records)
    throws StepException, DirectiveParseException {
    TextDirectives specification = new TextDirectives(directives);
    List<Step> steps = new ArrayList<>();
    steps.addAll(specification.getSteps());
    records = execute(steps, records);
    return records;
  }

  @Test
  public void testSchemaGeneration() throws Exception {
    Map<String, String> values = new HashMap<>(); values.put("foo", "1");
    JSONArray array1 = new JSONArray(); array1.put("a"); array1.put("b");
    JSONArray array2 = new JSONArray(); array2.put(1); array2.put(2);
    JSONArray array3 = new JSONArray(); array3.put(1.9); array3.put(4.5);
    JSONArray array4 = new JSONArray();
    JSONObject o1 = new JSONObject();
    o1.put("name", "test");
    o1.put("age", 34);
    o1.put("salary", 5.6);
    o1.put("address", JSONObject.NULL);
    JSONObject o2 = new JSONObject();
    o2.put("name", "joltie");
    o2.put("age", 22);
    o2.put("salary", JSONObject.NULL);
    o2.put("address", "5787 Mars Ave, Mars, 34242");
    array4.put(o1);
    array4.put(o2);

    Record record = new Record("values", values).add("int", 1).add("string", "2").add("double", 2.3).add("array1", array1)
      .add("array2", array2).add("array3", array3).add("arrayobject", array4);
    Schema schema = SchemaUtilities.recordToSchema("record", Arrays.asList(record));
    Assert.assertNotNull(schema);
  }
}
