lexer grammar DirectivesLexer;

// Existing rules remain unchanged

// Add these new fragments and rules
fragment DIGIT : [0-9];
fragment BYTE_UNIT : 'B'|'KB'|'MB'|'GB'|'TB';
fragment TIME_UNIT : 'ns'|'ms'|'s'|'m'|'h';

BYTE_SIZE : DIGIT+ BYTE_UNIT;
TIME_DURATION : DIGIT+ TIME_UNIT;

// Update parser rules (add to existing grammar)
byteSizeArg : BYTE_SIZE;
timeDurationArg : TIME_DURATION;