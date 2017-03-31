package co.cask.wrangler.service.recipe;

import co.cask.cdap.internal.guava.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Recipe pojo that stores information about a recipe in the backend dataset.
 */
public final class RecipeDatum {
  private static final Gson gson = new GsonBuilder().create();
  // Id of the recipe.
  private String id;

  // Unix timestamp about when the recipe was created.
  private long created;

  // Unix timestamp about when the recipe was last updated.
  private long updated;

  // List of directives that make-up recipe.
  private String directives;

  // Name of the recipe - display name.
  private String name;

  // Description for the recipe.
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

  /**
   * @return recipe id.
   */
  public String getId() {
    return id;
  }

  /**
   * Sets the recipe id for the recipe.
   * @param id of the recipe.
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * @return Unix timestamp about when the recipe was created.
   */
  public long getCreated() {
    return created;
  }

  /**
   * Sets the time of when the recipe was created.
   *
   * @param created unix timestamp of when recipe was created.
   */
  public void setCreated(long created) {
    this.created = created;
  }

  /**
   * @return Unix timestamp about when the recipe was updated.
   */
  public long getUpdated() {
    return updated;
  }

  /**
   * Sets the time when the recipe was updated.
   * @param updated unix timestamp of when recipe was updated.
   */
  public void setUpdated(long updated) {
    this.updated = updated;
  }

  /**
   * @return List of directives that is part of recipe.
   */
  public List<String> getDirectives() {
    if (directives != null) {
      return gson.fromJson(directives, new TypeToken<List<String>>() {}.getType());
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * Sets the list of directives that make a recipe.
   *
   * @param directives List of directives that are part of recipe.
   */
  public void setDirectives(List<String> directives) {
    this.directives = gson.toJson(directives);
  }

  /**
   * @return Display name of the directive.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the display name of the directive.
   *
   * @param name to be displayed for the directive.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return Description associated with the directive.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Sets the recipe description.
   *
   * @param description for the recipe.
   */
  public void setDescription(String description) {
    this.description = description;
  }
}
