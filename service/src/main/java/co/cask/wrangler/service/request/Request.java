package co.cask.wrangler.service.request;

import co.cask.wrangler.service.request.v1.Recipe;
import co.cask.wrangler.service.request.v1.Sampling;
import co.cask.wrangler.service.request.v1.Workspace;

/**
 * Created by nitin on 3/24/17.
 */
public interface Request {
  int getVersion();
  Workspace getWorkspace();
  Sampling getSampling();
  Recipe getRecipe();
}
