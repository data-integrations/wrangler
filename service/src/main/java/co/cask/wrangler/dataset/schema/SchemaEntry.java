package co.cask.wrangler.dataset.schema;

import java.util.Set;

/**
 * Schema Entry
 */
public final class SchemaEntry {
  private final String id;
  private final String name;
  private final String description;
  private final SchemaDescriptorType type;
  private final Set<Long> versions;
  private final byte[] specification;
  private final long current;

  public SchemaEntry(String id, String name, String description, SchemaDescriptorType type,
                     Set<Long> versions, byte[] specification, long current) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.type = type;
    this.versions = versions;
    this.specification = specification;
    this.current = current;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public SchemaDescriptorType getType() {
    return type;
  }

  public Set<Long> getVersions() {
    return versions;
  }

  public byte[] getSpecification() {
    return specification;
  }

  public long getCurrent() {
    return current;
  }
}
