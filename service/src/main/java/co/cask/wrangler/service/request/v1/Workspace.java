package co.cask.wrangler.service.request.v1;

import com.google.gson.annotations.Since;

/**
 * Created by nitin on 3/24/17.
 */
public class Workspace {
  @Since(1.0)
  private String name;

  @Since(1.0)
  private Integer results;
}
