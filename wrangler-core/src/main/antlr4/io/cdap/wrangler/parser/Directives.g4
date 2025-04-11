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
}

recipe : statements EOF ;

statements : (Comment | macro | directive SColon | pragma SColon | ifStatement)* ;

directive
 : command (codeblock
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
            | properties)*?
 ;

ifStatement : ifStat elseIfStat* elseStat? CBrace ;
ifStat      : IF expression OBrace statements ;
elseIfStat  : CBrace ELSE IF expression OBrace statements ;
elseStat    : CBrace ELSE OBrace statements ;

expression  : OParen (~OParen | expression)* CParen ;

forStatement : FOR OParen Identifier Assign expression SColon expression SColon expression CParen OBrace statements CBrace ;

macro : Dollar OBrace (~OBrace | macro | Macro)*? CBrace ;

pragma : PRAGMA (pragmaLoadDirective | pragmaVersion) ;
pragmaLoadDirective : LOAD_DIRECTIVES identifierList ;
pragmaVersion : VERSION Number ;

codeblock : EXP Space* Colon condition ;

identifier : Identifier ;

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
 : Identifier Assign (text | number | bool) 
 ;

numberRanges 
 : numberRange (Comma numberRange)* 
 ;

numberRange  
: Number Colon Number Assign value 
;

value 
    : text
    | number 
    | Column 
    | Bool 
    | BYTE_SIZE 
    | TIME_DURATION 
    ;

ecommand : External Identifier ;

config 
: Identifier ;

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

// Lexer rules
OBrace   : '{';
CBrace   : '}';
SColon   : ';';
Or       : '||';
And      : '&&';
Equals   : '==';
NEquals  : '!=';
GTEquals : '>=';
LTEquals : '<=';
Match    : '=~';
NotMatch : '!~';
QuestionColon : '?:';
StartsWith : '=^';
NotStartsWith : '!^';
EndsWith : '=$';
NotEndsWith : '!$';
PlusEqual : '+=';
SubEqual : '-=';
MulEqual : '*=';
DivEqual : '/=';
PerEqual : '%=';
AndEqual : '&=';
OrEqual  : '|=';
XOREqual : '^=';
Pow      : '^';
External : '!';
GT       : '>';
LT       : '<';
Add      : '+';
Subtract : '-';
Multiply : '*';
Divide   : '/';
Modulus  : '%';
OBracket : '[';
CBracket : ']';
OParen   : '(';
CParen   : ')';
Assign   : '=';
Comma    : ',';
QMark    : '?';
Colon    : ':';
Dot      : '.';
At       : '@';
Pipe     : '|';
BackSlash: '\\';
Dollar   : '$';
Tilde    : '~';

// Keywords
IF              : 'if';
ELSE            : 'else';
FOR             : 'for';
PRAGMA          : '#pragma';
LOAD_DIRECTIVES : 'load-directives';
VERSION         : 'version';
EXP             : 'exp';
PROP            : 'prop';

Bool 
: 'true' 
| 'false' 
;

Number 
: Int (Dot Digit*)? 
;

Identifier 
: [a-zA-Z_\-] [a-zA-Z_0-9\-]* 
;

Macro      
: [a-zA-Z_] [a-zA-Z_0-9]* 
;

Column     
: ':' [a-zA-Z_\-] [:a-zA-Z_0-9\-]* 
;

String 
: '\'' ( EscapeSequence | ~('\'') )* '\''
| '"'  ( EscapeSequence | ~('"') )* '"' 
;

// Byte size units
BYTE_SIZE : Digit+ (Dot Digit+)? BYTE_UNIT ;
fragment BYTE_UNIT : [kK][bB]? | [mM][bB]? | [gG][bB]? ;

// Time duration units
TIME_DURATION : Digit+ (Dot Digit+)? TIME_UNIT ;
fragment TIME_UNIT : [mM][sS] | [sS](ec(onds)?)? ;

EscapeSequence : '\\' ('b'|'t'|'n'|'f'|'r'|'"'|'\''|'\\')
               | UnicodeEscape
               | OctalEscape ;

fragment 
OctalEscape 
  : '\\' ('0'..'3') ('0'..'7') ('0'..'7')
  | '\\' ('0'..'7') ('0'..'7')
  | '\\' ('0'..'7') 
  ;

fragment
UnicodeEscape 
: '\\' 'u' HexDigit HexDigit HexDigit HexDigit 
;

fragment 
  HexDigit : ('0'..'9'|'a'..'f'|'A'..'F') ;

Comment 
: ('//' ~[\r\n]* | '/*' .*? '*/' | '--' ~[\r\n]* ) -> skip 
;

Space   
: [ \t\r\n\u000C]+ -> skip 
;

fragment Int 
: '-'? [1-9] Digit* [L]* | '0' 
;

fragment Digit 
: [0-9] 
;
