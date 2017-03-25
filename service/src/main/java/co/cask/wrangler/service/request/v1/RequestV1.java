package co.cask.wrangler.service.request.v1;

import co.cask.wrangler.service.request.Request;

/**
 * Created by nitin on 3/24/17.
 */
public final class RequestV1 implements Request {
  private int version;
  private Workspace workspace;
  private Recipe recipe;
  private Sampling sampling;

  public RequestV1(int version, Workspace workspace, Recipe recipe, Sampling sampling) {
    this.version = version;
    this.workspace = workspace;
    this.recipe = recipe;
    this.sampling = sampling;
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public Workspace getWorkspace() {
    return workspace;
  }

  @Override
  public Sampling getSampling() {
    return sampling;
  }

  @Override
  public Recipe getRecipe() {
    return recipe;
  }
}
