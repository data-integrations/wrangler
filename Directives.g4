// Add to lexer rules
BYTE_UNIT: ('B' | 'KB' | 'MB' | 'GB' | 'TB');
TIME_UNIT: ('ms' | 's' | 'min' | 'h');

BYTE_SIZE: [0-9]+('.'[0-9]+)? BYTE_UNIT;
TIME_DURATION: [0-9]+('.'[0-9]+)? TIME_UNIT;

// Update the parser rule
value
  : BYTE_SIZE     # byteSizeArg
  | TIME_DURATION # timeDurationArg
  | ...           // existing rules
  ;
