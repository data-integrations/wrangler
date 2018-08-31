package co.cask.wrangler.service;

/**
 * Plugin configuration specifying the name, type and properties of the plugin
 * @param <T>
 */
public class PluginConfiguration<T> {
  private final String name;
  private final String type;
  private final T properties;

  public PluginConfiguration(String name, String type, T properties) {
    this.name = name;
    this.type = type;
    this.properties = properties;
  }
}
