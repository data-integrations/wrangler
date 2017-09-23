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

package co.cask.wrangler.service.directive;

import co.cask.wrangler.proto.Recipe;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.proto.RequestV1;
import co.cask.wrangler.proto.Sampling;
import co.cask.wrangler.proto.Workspace;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

/**
 * Serializes the HTTP Request received by the service.
 */
public class RequestDeserializer implements JsonDeserializer<Request> {
  @Override
  public Request deserialize(JsonElement json, Type type, JsonDeserializationContext context)
    throws JsonParseException {

    final JsonObject object = json.getAsJsonObject();

    // If the version is not specified
    if (!object.has("version")) {
      throw new JsonParseException(
        String.format("Version field is not specified in the request.")
      );
    }

    int version = object.get("version").getAsInt();

    if (version == 1) {
      Workspace workspace = context.deserialize(object.get("workspace"), Workspace.class);
      Recipe recipe = context.deserialize(object.get("recipe"), Recipe.class);
      Sampling sampling = context.deserialize(object.get("sampling"), Sampling.class);
      JsonObject properties = context.deserialize(object.get("properties"), JsonObject.class);
      return new RequestV1(version, workspace, recipe, sampling, properties);
    } else {
      throw new JsonParseException (
        String.format("Unsupported request version %d.", version)
      );
    }
  }
}
