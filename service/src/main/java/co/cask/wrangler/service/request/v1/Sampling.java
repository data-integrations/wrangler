package co.cask.wrangler.service.request.v1;

/**
 * Created by nitin on 3/24/17.
 */
public final class Sampling {
  private String method;
  private Integer seed;
  private Integer limit;

  public String getMethod() {
    return method;
  }

  public Integer getSeed() {
    return seed;
  }

  public Integer getLimit() {
    return limit;
  }
}
