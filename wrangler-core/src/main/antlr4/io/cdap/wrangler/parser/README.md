# Wrangler Directive Grammar

This directory contains the ANTLR4 grammar definition for the Wrangler directive language.

## Directives.g4

The `Directives.g4` file defines the grammar for parsing Wrangler directives. The grammar is used to
generate the lexer and parser for interpreting directive recipes.

### Grammar Structure

The grammar consists of:

1. **Lexer Rules**: Define tokens like identifiers, numbers, strings, etc.
2. **Parser Rules**: Define the structure of directives, expressions, and other language constructs.

### Key Grammar Elements

- **Recipe**: Top-level construct representing a complete set of directives
- **Directive**: Individual data transformation command with its arguments
- **Pragma**: Special instructions like loading custom directives or setting versions
- **Expression**: Code blocks containing conditions or calculations
- **Control Structures**: If-else statements and loops for conditional execution

### Data Type Token Support

The grammar supports various data type tokens, including:

- **ByteSize**: Values representing sizes with byte units (e.g., "10KB", "1.5MB")
- **TimeDuration**: Values representing time durations with units (e.g., "100ms", "5s")

These are defined in the lexer section with specific patterns:

```antlr
ByteSize
 : Number ByteUnit
 ;

TimeDuration
 : Number TimeUnit
 ;

fragment ByteUnit
 : [kK][bB]  // Kilobyte
 | [mM][bB]  // Megabyte
 | [gG][bB]  // Gigabyte
 | [tT][bB]  // Terabyte
 | [pP][bB]  // Petabyte
 | [bB]      // Byte
 ;

fragment TimeUnit
 : [nN][sS]        // Nanoseconds
 | [mM][sS]        // Milliseconds
 | [sS]            // Seconds
 | [mM]            // Minutes
 | [hH]            // Hours
 | [dD]            // Days
 ;
```

### How It's Used

The ANTLR tool processes this grammar to generate Java classes that:

1. Break input text into tokens (lexer)
2. Build a parse tree from those tokens (parser)
3. Allow traversal of the parse tree to interpret and execute directives

The generated parser is used by Wrangler's directive system to process user-provided
transformation recipes.