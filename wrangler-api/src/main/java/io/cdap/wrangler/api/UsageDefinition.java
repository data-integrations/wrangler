@@ -47,11 +47,15 @@ public final class UsageDefinition implements Serializable {
  private final transient int optionalCnt;
  private final String directive;
  private final List<TokenDefinition> tokens;
  private final int BYTE_SIZE;
  private final int TIME_DURATION;

  private UsageDefinition(String directive, int optionalCnt, List<TokenDefinition> tokens) {
    this.directive = directive;
    this.tokens = tokens;
    this.optionalCnt = optionalCnt;
    this.TIME_DURATION = 0;
    this.BYTE_SIZE = 0;
  }

  /**
