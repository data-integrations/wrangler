## 🔧 Enhancements for Assignment

This fork adds support for:

- **ByteSize Token** – Parses values like "10KB", "2MB", "1.5GB"
- **TimeDuration Token** – Parses values like "100ms", "2s", "5min"
- **New Directive `aggregate-stats`**:
    - Usage:
      ```
      aggregate-stats :col1 :col2 total_bytes total_time
      ```
    - Aggregates total size and time from selected columns
    - Supports output in MB, GB, seconds, etc.

Files Modified:
- `Directives.g4` (Lexer & grammar)
- `ByteSize.java`, `TimeDuration.java` (Token extensions)
- `aggregate-stats.java` (new directive)
- Unit tests for new tokens and directive
