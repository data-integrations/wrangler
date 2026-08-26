/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

grammar Directives;

options {
  language = Java;
}

@lexer::header {
/*
 * Copyright © 2017-2019 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0
 */
}

recipe
 : statements EOF
 ;

statements
 : (Comment | macro | directive SColon | pragma SColon | ifStatement)*
 ;

directive
 : command
   ( codeblock
   | identifier
   | macro
   | text
   | number
   | bool
   | column
   | colList
   | numberList
   | boolList
   | stringList
   | numberRanges
   | properties
   )*?
 ;

ifStatement
  : ifStat elseIfStat* elseStat? CBrace
  ;

ifStat
  : IF expression OBrace statements
  ;

elseIfStat
  : CBrace ELSE IF expression OBrace statements
  ;

elseStat
  : CBrace ELSE OBrace statements
  ;

expression
  : OParen (~OParen | expression)* CParen
  ;

forStatement
 : FOR OParen Identifier Assign expression SColon expression SColon expression CParen OBrace statements CBrace
 ;

macro
 : Dollar OBrace (~OBrace | macro | Macro)*? CBrace
 ;

pragma
 : PRAGMA (pragmaLoadDirective | pragmaVersion)
 ;

pragmaLoadDirective
 : LOAD_DIRECTIVES identifierList
 ;

pragmaVersion
 : VERSION Number
 ;

codeblock
 : EXP Space* Colon condition
 ;

identifier
 : Identifier
 ;

properties
 : PROP Colon OBrace (propertyList)+ CBrace
   | PROP Colon OBrace OBrace (propertyList)+ CBrace { notifyErrorListeners("Too many start paranthesis"); }
   | PROP Colon OBrace (propertyList)+ CBrace CBrace { notifyErrorListeners("Too many start paranthesis"); }
   | PROP Colon (propertyList)+ CBrace { notifyErrorListeners("Missing opening brace"); }
   | PROP Colon OBrace (propertyList)+ { notifyErrorListeners("Missing closing brace"); }
 ;

propertyList
 : property (Comma property)*
 ;

property
 : Identifier (Assign | Colon) (text | number | bool)
 ;


numberRanges
 : numberRange (Comma numberRange)*
 ;

numberRange
 : Number Colon Number Assign value
 ;

value
 : STRING
 | FLOAT
 | INT
 | BYTE_SIZE {
     String text = $BYTE_SIZE.text.toLowerCase();
     if (!text.matches("^-?\\d+(\\.\\d+)?(\\s)?(b|kb|mb|gb|tb)$")) {
         notifyErrorListeners("Invalid BYTE_SIZE format: '" + $BYTE_SIZE.text + "'. Allowed units: B, KB, MB, GB, TB.");
     }
   }
 | TIME_DURATION {
     String text = $TIME_DURATION.text.toLowerCase();
     if (!text.matches("^-?\\d+(\\.\\d+)?(\\s)?(ms|s|m|h)$")) {
         notifyErrorListeners("Invalid TIME_DURATION format: '" + $TIME_DURATION.text + "'. Allowed units: ms, s, m, h.");
     }
   }
 ;

ecommand
 : External Identifier
 ;

config
 : Identifier
 ;

column
 : Column
 ;

text
 : String
 ;

number
 : Number
 ;

bool
 : Bool
 ;

condition
 : OBrace (~CBrace | condition)* CBrace
 ;

command
 : Identifier
 ;

colList
 : Column (Comma Column)+
 ;

numberList
 : Number (Comma Number)+
 ;

boolList
 : Bool (Comma Bool)+
 ;

stringList
 : String (Comma String)+
 ;

identifierList
 : Identifier (Comma Identifier)*
 ;

// Keyword tokens
IF      : 'if';
ELSE    : 'else';
FOR     : 'for';
VERSION : 'version';
PRAGMA  : '#pragma';
LOAD_DIRECTIVES : 'load-directives';
PROP    : 'prop';
EXP     : 'exp';

// Symbols
OBrace   : '{';
CBrace   : '}';
SColon   : ';';
Colon    : ':';
Comma    : ',';
Assign   : '=';
OParen   : '(';
CParen   : ')';
External : '!';
Dollar   : '$';
Space    : [ \t]+ -> skip;

// Operators
Or       : '||';
And      : '&&';
Equals   : '==';
NEquals  : '!=';
GTEquals : '>=';
LTEquals : '<=';
GT       : '>';
LT       : '<';
Add      : '+';
Subtract : '-';
Multiply : '*';
Divide   : '/';
Modulus  : '%';

// Types
Bool     : 'true' | 'false';
Number   : Int ('.' Digit*)?;
Identifier : [a-zA-Z_\-] [a-zA-Z_0-9\-]*;
Macro    : [a-zA-Z_] [a-zA-Z_0-9]*;
Column   : ':' [a-zA-Z_\-] [:a-zA-Z_0-9\-]*;
String   : '\'' ( EscapeSequence | ~('\'') )* '\''
         | '"'  ( EscapeSequence | ~('"') )* '"';

EscapeSequence
 : '\\' ('b'|'t'|'n'|'f'|'r'|'"'|'\''|'\\')
 | UnicodeEscape
 | OctalEscape
 ;

fragment OctalEscape
 : '\\' ('0'..'3') ('0'..'7') ('0'..'7')
 | '\\' ('0'..'7') ('0'..'7')
 | '\\' ('0'..'7')
 ;

fragment UnicodeEscape
 : '\\' 'u' HexDigit HexDigit HexDigit HexDigit
 ;

fragment HexDigit : ('0'..'9'|'a'..'f'|'A'..'F');

Comment : ('//' ~[\r\n]* | '/*' .*? '*/' | '--' ~[\r\n]*) -> skip;

fragment Int : '-'? [1-9] Digit* [L]* | '0';
fragment Digit : [0-9];

// Byte Size
BYTE_SIZE: DIGIT+ ('.' DIGIT+)? BYTE_UNIT;
fragment BYTE_UNIT: 'B' | 'KB' | 'MB' | 'GB' | 'TB' | 'PB';


// Time Duration
TIME_DURATION: DIGIT+ ('.' DIGIT+)? TIME_UNIT;
fragment TIME_UNIT: 'ns' | 'us' | 'ms' | 's' | 'm' | 'h' | 'd';


// Digits (used in fragments)
fragment DIGIT: [0-9];


aggregate_stats:
    'aggregate_stats' '(' ('byte_size' | 'time_duration') ')'
;

