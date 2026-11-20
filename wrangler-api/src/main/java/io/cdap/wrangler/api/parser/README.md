# Parser API for Wrangler

This package contains the core interfaces and classes for parsing directives in the Wrangler framework.

## TokenType.java

The `TokenType` enum defines all the supported token types in the Wrangler directive grammar. Each token type
corresponds to a specific format of data recognized by the parser.

### Token Types

| Token Type | Description | Example |
|------------|-------------|---------|
| `DIRECTIVE_NAME` | Name of a directive | `parse-as-csv`, `drop` |
| `COLUMN_NAME` | Reference to a column | `:body`, `:timestamp` |
| `TEXT` | String literal | `'Hello'`, `"World"` |
| `NUMERIC` | Numeric value | `42`, `3.14` |
| `BOOLEAN` | Boolean value | `true`, `false` |
| `COLUMN_NAME_LIST` | List of column names | `:col1,:col2,:col3` |
| `TEXT_LIST` | List of text values | `'val1','val2','val3'` |
| `NUMERIC_LIST` | List of numeric values | `1,2,3,4` |
| `BOOLEAN_LIST` | List of boolean values | `true,false,true` |
| `EXPRESSION` | Code block expression | `exp:{ value > 100 }` |
| `PROPERTIES` | Key-value properties | `prop:{ key1='val1', key2=5 }` |
| `RANGES` | Range-based mapping | `1:5='small',6:10='medium'` |
| `IDENTIFIER` | Alphanumeric identifier | `myVar`, `column_name` |
| `BYTE_SIZE` | Size with byte unit | `10KB`, `1.5MB`, `2GB` |
| `TIME_DURATION` | Duration with time unit | `100ms`, `5s`, `2.5h` |

### Implementations

Each token type has an associated implementation class in the `io.cdap.wrangler.api.parser.token` package:

- `BYTE_SIZE` is implemented by `ByteSize`
- `TIME_DURATION` is implemented by `TimeDuration`
- Other token types are implemented by their respective classes

These token types are used by the Wrangler grammar (Directives.g4) to recognize and parse directive components
during recipe execution.