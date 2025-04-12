grammar Directives;

// Define a rule for a list of directives
directives : directive (EOL directive)* EOF ;

// A directive is a name followed by optional arguments
directive : name=IDENTIFIER (WS+ arguments)? ;

// Arguments are a list of one or more values
arguments : value (WS+ value)* ;

// A value can be a string, identifier, column reference, byte size, time duration, or property
value : STRING_LITERAL                                       # String
      | COLUMN_NAME                                          # Column
      | IDENTIFIER                                           # Identifier
      | INTEGER                                              # Integer
      | DECIMAL                                              # Decimal
      | BYTE_SIZE                                            # ByteSize
      | TIME_DURATION                                        # TimeDuration
      | PROPERTY                                             # Property
      ;

// Lexer Rules
DIGIT : [0-9] ;
WS : [ \t]+ ;
EOL : [\r\n]+ ;

PROPERTY : (IDENTIFIER '=' (IDENTIFIER | STRING_LITERAL)) ;

// Identifiers start with a letter or underscore
IDENTIFIER : [a-zA-Z_] [a-zA-Z0-9_]* ;

// Column names are prefixed with a colon
COLUMN_NAME : ':' [a-zA-Z_] [a-zA-Z0-9_]* ;

// Numeric literals
INTEGER : DIGIT+ ;
DECIMAL : DIGIT+ '.' DIGIT* | '.' DIGIT+ ;

// String literals enclosed in single or double quotes
STRING_LITERAL : '\'' ( ~'\'' | '\\' '\'' )* '\''
               | '"' ( ~'"' | '\\' '"' )* '"'
               ;

// Byte size literals (e.g., 5MB, 1.5GB)
BYTE_SIZE : (INTEGER | DECIMAL) WS? ('B'|'KB'|'MB'|'GB'|'TB'|'PB') ;

// Time duration literals (e.g., 100ms, 5s, 1.5h)
TIME_DURATION : (INTEGER | DECIMAL) WS? ('ns'|'ms'|'s'|'m'|'h'|'d') ;

// Skip comments
COMMENT : '#' ~[\r\n]* -> skip ;
