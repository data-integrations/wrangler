@@ -140,7 +140,12 @@ numberRange
 ;

value
 : String | Number | Column | Bool
 : String
 | Number
 | Column
 | Bool
 | BYTE_SIZE
 | TIME_DURATION
 ;

ecommand
@@ -256,6 +261,26 @@ Bool
Number
 : Int ('.' Digit*)?
 ;
 BYTE_SIZE
   : Int BYTE_UNIT
   ;

 fragment BYTE_UNIT
   : [KMGTP] 'B'     // KB, MB, GB, TB, PB
   | 'B'             // Bytes
   ;

 TIME_DURATION
   : Int TIME_UNIT
   ;

 fragment TIME_UNIT
   : 'ms'            // milliseconds
   | 's'             // seconds
   | 'm'             // minutes
   | 'h'             // hours
   | 'd'             // days
   ;

Identifier
 : [a-zA-Z_\-] [a-zA-Z_0-9\-]*
