grammar Directives;

// Fragments
fragment DIGIT: [0-9];
fragment WS: [ \t\r\n]+;
fragment LETTER: [a-zA-Z];
fragment ESC: '\\' .;
fragment STRING_CHAR: ~["\\\r\n] | ESC;

// Fragments for units
fragment BYTE_UNIT: 'B' | 'KB' | 'MB' | 'GB' | 'TB' | 'PB' | 'KiB' | 'MiB' | 'GiB' | 'TiB' | 'PiB';
fragment TIME_UNIT: 'ns' | 'μs' | 'ms' | 's' | 'm' | 'h' | 'd';

// Lexer rules
STRING: '"' STRING_CHAR* '"' | '\'' STRING_CHAR* '\'';
NUMBER: DIGIT+ ('.' DIGIT+)?;
BOOLEAN: 'true' | 'false';
NULL: 'null';
IDENTIFIER: LETTER (LETTER | DIGIT | '_')*;

// Lexer rules for size and time
BYTE_SIZE: DIGIT+ ('.' DIGIT+)? WS* BYTE_UNIT;
TIME_DURATION: DIGIT+ ('.' DIGIT+)? WS* TIME_UNIT;

// Parser rules
byteSize: BYTE_SIZE;
timeDuration: TIME_DURATION;

// Add to value rule
value: 
    STRING
    | NUMBER
    | BOOLEAN
    | byteSize
    | timeDuration
    | NULL
    ; 