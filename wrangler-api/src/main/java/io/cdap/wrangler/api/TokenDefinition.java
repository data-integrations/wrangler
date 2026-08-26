@@ -45,13 +45,17 @@ public final class TokenDefinition implements Serializable {
  private final String name;
  private final TokenType type;
  private final String label;
  private final int BYTE_SIZE;
  private final int TIME_DURATION;

  public TokenDefinition(String name, TokenType type, String label, int ordinal, boolean optional) {
    this.name = name;
    this.type = type;
    this.label = label;
    this.ordinal = ordinal;
    this.optional = optional;
    this.BYTE_SIZE=0;
    this.TIME_DURATION=0;
  }

  /**
