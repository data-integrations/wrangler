BYTE_SIZE
  : Number BYTE_UNIT
  ;

TIME_DURATION
  : Number TIME_UNIT
  ;

// Fragments for unit matching
fragment BYTE_UNIT
  : [kKmMgGtTpPeE]? [bB]
  ;

fragment TIME_UNIT
  : 'ms' | 's' | 'm' | 'h' | 'd'
  ;
