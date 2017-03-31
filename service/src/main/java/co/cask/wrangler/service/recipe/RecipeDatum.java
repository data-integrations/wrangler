package co.cask.wrangler.service.recipe;

import co.cask.cdap.internal.guava.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public final class RecipeDatum {
  private static final Gson gson = new GsonBuilder().create();
  private String id;
  private long created;
  private long updated;
  private String directives;
  private String name;
  private String description;

  public RecipeDatum(String id) {
    this.id = id;
    this.created = System.currentTimeMillis() / 1000;
    this.updated = created;
    this.directives = null;
    this.name = "No name";
    this.description = "No description";
  }

  public RecipeDatum(String id, String name) {
    this(id);
    this.name = name;
  }

  public RecipeDatum(String id, String name, String description) {
    this(id, name);
    this.description = description;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getCreated() {
    return created;
  }

  public void setCreated(long created) {
    this.created = created;
  }

  public long getUpdated() {
    return updated;
  }

  public void setUpdated(long updated) {
    this.updated = updated;
  }

  public List<String> getDirectives() {
    if (directives != null) {
      return gson.fromJson(directives, new TypeToken<List<String>>() {}.getType());
    } else {
      return new ArrayList<>();
    }
  }

  public void setDirectives(List<String> directives) {
    this.directives = gson.toJson(directives);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
