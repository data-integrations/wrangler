/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.wrangler.service.directive;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.Row;
import org.apache.commons.lang3.ClassUtils;

import java.io.IOException;

/**
 * Formats Structure and Array records in a "pseudojson" in which it collapses
 * complex types by callint toString().  This is done so the complex types are shown
 * in a similar way inside and outside the structure records.
 */
public class WranglerDisplaySerializer implements TypeAdapterFactory {

  public static final String NONDISPLAYABLE_STRING = "Non-displayable object";

  @Override
  public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
    // Let GSON Handle primitives and collections
    if (ClassUtils.isPrimitiveOrWrapper(type.getRawType())
      || (Iterable.class.isAssignableFrom(type.getRawType()))) {
      return null;
    }
    if (Row.class.isAssignableFrom(type.getRawType())) {
      return (TypeAdapter<T>) rowClassAdapter(gson);
    }

    return (TypeAdapter<T>) defaultClassAdapter(gson);
  }

  private TypeAdapter<Row> rowClassAdapter(Gson gson) {
    final TypeAdapter<Object> elementAdapter = gson.getAdapter(Object.class);

    return new TypeAdapter<Row>() {
      @Override
      public void write(JsonWriter out, Row value) throws IOException {
        if (value == null) {
          return;
        }
        out.beginObject();
        for (Pair<String, Object> field : value.getFields()) {
          out.name(field.getFirst());
          elementAdapter.write(out, field.getSecond());
        }
        out.endObject();
      }

      @Override public
      Row read(JsonReader in) throws IOException {
        throw new UnsupportedOperationException("Reading Rows from Wrangler display format not supported.");
      }
    };
  }


  private TypeAdapter<Object> defaultClassAdapter(Gson gson) {
    return new TypeAdapter<Object>() {
      @Override
      public void write(JsonWriter out, Object value) throws IOException {
        try {
          if ((value.getClass().getMethod("toString").getDeclaringClass() != Object.class)) {
            out.value(value.toString());
          } else {
            out.value(NONDISPLAYABLE_STRING);
          }
        } catch (NoSuchMethodException e) {
          out.value(NONDISPLAYABLE_STRING);
        }
      }

      @Override public
      Object read(JsonReader in) throws IOException {
        throw new UnsupportedOperationException("Can't read object from it's string implementation");
      }
    };
  }
}
