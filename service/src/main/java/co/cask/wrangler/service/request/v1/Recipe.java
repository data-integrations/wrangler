package co.cask.wrangler.service.request.v1;

/**
 * Created by nitin on 3/24/17.
 */
public class Recipe {
  private String[] directives;
  private Boolean save;
  private String name;

  public Recipe(String[] directives, Boolean save, String name) {
    this.directives = directives;
    this.save = save;
    this.name = name;
  }

  public String[] getDirectives() {
    return directives;
  }

  public Boolean getSave() {
    return save;
  }

  public String getName() {
    return name;
  }
}
