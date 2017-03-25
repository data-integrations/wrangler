package co.cask.wrangler.service.request.v1;

import com.google.gson.annotations.Since;

/**
 * Created by nitin on 3/24/17.
 */
public final class Sampling {
  @Since(1.0)
  private String method;

  @Since(1.0)
  private Integer seed;

  @Since(1.0)
  private Integer limit;
}
