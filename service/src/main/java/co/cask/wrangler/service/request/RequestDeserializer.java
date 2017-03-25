package co.cask.wrangler.service.request;

import co.cask.wrangler.service.request.v1.Recipe;
import co.cask.wrangler.service.request.v1.RequestV1;
import co.cask.wrangler.service.request.v1.Sampling;
import co.cask.wrangler.service.request.v1.Workspace;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

public class RequestDeserializer implements JsonDeserializer<Request> {
  @Override
  public Request deserialize(JsonElement json, Type type, JsonDeserializationContext context)
    throws JsonParseException {

    final JsonObject object = json.getAsJsonObject();

    // If the version is not specified
    if (!object.has("version")) {
      throw new JsonParseException(
        String.format("RequestV1 version is not specified.")
      );
    }

    int version = object.get("version").getAsInt();
    if (version == 1) {
      Workspace workspace = context.deserialize(object.get("workspace"), Workspace.class);
      Recipe recipe = context.deserialize(object.get("recipe"), Recipe.class);
      Sampling sampling = context.deserialize(object.get("sampling"), Sampling.class);
      return new RequestV1(version, workspace, recipe, sampling);
    } else {
      throw new JsonParseException (
        String.format("Unsupported version '%d' of request.", version)
      );
    }

  }
}