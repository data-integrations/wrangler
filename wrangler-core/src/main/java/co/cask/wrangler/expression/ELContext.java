package co.cask.wrangler.expression;

import org.apache.commons.jexl3.JexlContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Class description here.
 */
public class ELContext implements JexlContext {
  private final Map<String, Object> values = new HashMap<>();

  public ELContext() {
    // no-op
  }

  public ELContext(String name, Object object) {
    values.put(name, object);
  }

  public ELContext(Map<String, Object> values) {
    this.values.putAll(values);
  }

  @Override
  public Object get(String name) {
    return values.get(name);
  }

  @Override
  public void set(String name, Object value) {
    values.put(name, value);
  }

  public ELContext add(String name, Object value) {
    values.put(name, value);
    return this;
  }


  @Override
  public boolean has(String name) {
    return values.containsKey(name);
  }
}
