package co.cask.wrangler.internal;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by nitin on 3/6/17.
 */
public final class RecordToStructuredRecord {
  private static final Logger LOG = LoggerFactory.getLogger(RecordToStructuredRecord.class);

  /**
   * Converts a Wrangler {@link Record} into a {@link StructuredRecord}.
   *
   * @param record defines a single {@link Record}
   * @param schema Schema associated with {@link StructuredRecord}
   * @return Populated {@link StructuredRecord}
   */
  public StructuredRecord convert(Record record, Schema schema) throws RecordConversionException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      Object value = record.getValue(name);
      builder.set(name, decode(name, value, field.getSchema()));
    }
    return builder.build();
  }

  /**
   * Converts a list of {@link Record} into populated list of {@link StructuredRecord}
   *
   * @param records Collection of records.
   * @param schema Schema associated with {@link StructuredRecord}
   * @return Populated list of {@link StructuredRecord}
   */
  public List<StructuredRecord> convert(List<Record> records, Schema schema) throws RecordConversionException {
    List<StructuredRecord> results = new ArrayList<>();
    for (Record record : records) {
      StructuredRecord r = convert(record, schema);
      results.add(r);
    }
    return results;
  }

  /**
   * @param name
   * @param object
   * @param schema
   * @return
   */
  private Object decode(String name, Object object, Schema schema) throws RecordConversionException {
    // Extract the type of the field.
    Schema.Type type = schema.getType();

    // Now based on the type, do the necessary decoding.
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
        //return decodeArray(name, (List)object, schema.getComponentSchema());
      case MAP:
        Schema key = schema.getMapSchema().getKey();
        Schema value = schema.getMapSchema().getValue();
        // Should be fine to cast since schema tells us what it is.
        //noinspection unchecked
        //return decodeMap(name, (Map<Object, Object>) object, key, value);
        break;
      case UNION:
        //return decodeUnion(name, object, schema.getUnionSchemas());
        break;
    }
    throw new RuntimeException("Unable decode object with schema " + schema);
  }



}
