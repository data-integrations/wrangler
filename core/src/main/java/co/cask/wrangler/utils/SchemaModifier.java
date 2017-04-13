package co.cask.wrangler.utils;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.data.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts an object with a schema into another type of object with the same schema.
 * For example, child implementations could convert a StructuredRecord to a GenericRecord and vice versa
 * as can be seen in AvroToStructuredTransformer and StructuredToAvroTransformer
 */
public final class SchemaModifier {

  public static Schema transform(Schema schema, Predicate<Schema.Field> predicate) throws IOException {
    List<Schema.Field> fields = new ArrayList<>();
    String name = schema.getRecordName();
    Schema.Type type = schema.getType();
    if (type == Schema.Type.RECORD) {
      for (Schema.Field field : schema.getFields()) {
        if (!predicate.apply(field)) {
          fields.add(Schema.Field.of(field.getName(), transform(field.getSchema(), predicate)));
        }
      }
      return Schema.recordOf(name, fields);
    } else if (type == Schema.Type.UNION) {
      List<Schema> sa = new ArrayList<>();
      for (Schema s : schema.getUnionSchemas()) {
        sa.add(transform(s, predicate));
      }
      return Schema.unionOf(sa);
    }
    return schema;
  }

}