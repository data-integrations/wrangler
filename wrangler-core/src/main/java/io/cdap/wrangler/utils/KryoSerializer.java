/*
 * Copyright © 2024 Cask Data, Inc.
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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.RemoteDirectiveResponse;
import io.cdap.wrangler.api.Row;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

/**
 * A helper class with allows Serialization and Deserialization using Kryo
 * We should register all schema classes present in {@link SchemaConverter}
 * and {@link RemoteDirectiveResponse}
 **/
public class KryoSerializer {

  private final Kryo kryo;
  private static final Gson GSON = new Gson();

  public KryoSerializer() {
    kryo = new Kryo();
    // Register all classes from RemoteDirectiveResponse
    kryo.register(RemoteDirectiveResponse.class);
    // Schema does not have no-arg constructor but implements Serializable
    kryo.register(Schema.class, new JavaSerializer());
    // Register all classes from SchemaConverter
    kryo.register(Row.class);
    kryo.register(ArrayList.class);
    kryo.register(LocalDate.class);
    kryo.register(LocalTime.class);
    kryo.register(ZonedDateTime.class);
    kryo.register(Map.class);
    kryo.register(JsonNull.class);
    // JsonPrimitive does not have no-arg constructor hence we need a
    // custom serializer as it is not serializable by JavaSerializer
    kryo.register(JsonPrimitive.class, new JsonSerializer());
    kryo.register(JsonArray.class);
    kryo.register(JsonObject.class);
    // Support deprecated util.date classes
    kryo.register(Date.class);
    kryo.register(java.sql.Date.class);
    kryo.register(Time.class);
    kryo.register(Timestamp.class);
  }

  public byte[] fromRemoteDirectiveResponse(RemoteDirectiveResponse response) {
    Output output = new Output(1024, -1);
    kryo.writeClassAndObject(output, response);
    return output.getBuffer();
  }

  public RemoteDirectiveResponse toRemoteDirectiveResponse(byte[] bytes) {
    Input input = new Input(bytes);
    return (RemoteDirectiveResponse) kryo.readClassAndObject(input);
  }

  static class JsonSerializer extends Serializer<JsonElement> {

    @Override
    public void write(Kryo kryo, Output output, JsonElement object) {
      output.writeString(GSON.toJson(object));
    }

    @Override
    public JsonElement read(Kryo kryo, Input input, Class<JsonElement> type) {
      return GSON.fromJson(input.readString(), type);
    }
  }
}
