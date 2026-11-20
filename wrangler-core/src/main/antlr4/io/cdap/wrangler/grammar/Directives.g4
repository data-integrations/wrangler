lexer grammar Directives;

// Lexer rules
BYTE_SIZE: DIGIT+ ('.' DIGIT+)? BYTE_UNIT;
TIME_DURATION: DIGIT+ ('.' DIGIT+)? TIME_UNIT;

fragment BYTE_UNIT: ('B' | 'KB' | 'MB' | 'GB' | 'TB');
fragment TIME_UNIT: ('ms' | 's' | 'm' | 'h');

// Parser rules
value
    : STRING
    | NUMBER
    | BOOLEAN
    | BYTE_SIZE
    | TIME_DURATION
    ;