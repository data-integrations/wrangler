package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.common.Bytes;
import co.cask.wrangler.steps.transformation.functions.Conversions;
import co.cask.wrangler.steps.transformation.functions.Dates;
import co.cask.wrangler.steps.transformation.functions.JSON;
import co.cask.wrangler.steps.transformation.functions.Types;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Collection of registered functions for jexl context.
 */
public final class JexlHelper {

  /**
   * @return configured {@link JexlEngine}
   */
  public static JexlEngine getEngine() {
    return new JexlBuilder()
      .namespaces(getRegisteredFunctions())
      .silent(false)
      .cache(10)
      .strict(true)
      .create();
  }

  /**
   * @return List of registered functions.
   */
  public static Map<String, Object> getRegisteredFunctions() {
    Map<String, Object> functions = new HashMap<>();
    functions.put(null, Conversions.class);
    functions.put("date", Dates.class);
    functions.put("json", JSON.class);
    functions.put("math", Math.class);
    functions.put("string", StringUtils.class);
    functions.put("bytes", Bytes.class);
    functions.put("arrays", Arrays.class);
    functions.put("types", Types.class);
    return functions;
  }
}
