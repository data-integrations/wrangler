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

/**
 * Parser Grammar for recognizing tokens and constructs of the directives language.
 */
recipe
 : statements EOF
 ;

statements
 :  ( Comment | macro | directive ';' | pragma ';' | ifStatement)*
 ;

directive
 : command
  (   codeblock
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
  : ifStat elseIfStat* elseStat? '}'
  ;

ifStat
  : 'if' expression '{' statements
  ;

elseIfStat
  : '}' 'else' 'if' expression '{' statements
  ;

elseStat
  : '}' 'else' '{' statements
  ;

expression
  : '(' (~'(' | expression)* ')'
  ;

forStatement
 : 'for' '(' Identifier '=' expression ';' expression ';' expression ')' '{'  statements '}'
 ;

macro
 : Dollar OBrace (~OBrace | macro | Macro)*? CBrace
 ;

pragma
 : '#pragma' (pragmaLoadDirective | pragmaVersion)
 ;

pragmaLoadDirective
 : 'load-directives' identifierList
 ;

pragmaVersion
 : 'version' Number
 ;

codeblock
 : 'exp' Space* ':' condition
 ;

identifier
 : Identifier
 ;

properties
 : 'prop' ':' OBrace (propertyList)+  CBrace
 | 'prop' ':' OBrace OBrace (propertyList)+ CBrace { notifyErrorListeners("Too many start paranthesis"); }
 | 'prop' ':' OBrace (propertyList)+ CBrace CBrace { notifyErrorListeners("Too many start paranthesis"); }
 | 'prop' ':' (propertyList)+ CBrace { notifyErrorListeners("Missing opening brace"); }
 | 'prop' ':' OBrace (propertyList)+  { notifyErrorListeners("Missing closing brace"); }
 ;

propertyList
 : property (',' property)*
 ;

property
 : Identifier '=' ( text | number | bool )
 ;

numberRanges
 : numberRange ( ',' numberRange)*
 ;

numberRange
 : Number ':' Number '=' value
 ;

value
 : String | Number | Column | Bool
 ;

ecommand
 : '!' Identifier
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
 : Column (','  Column)+
 ;

numberList
 : Number (',' Number)+
 ;

boolList
 : Bool (',' Bool)+
 ;

stringList
 : String (',' String)+
 ;

identifierList
 : Identifier (',' Identifier)*
 ;


/*
 * Following are the Lexer Rules used for tokenizing the recipe.
 */
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


Bool
 : 'true'
 | 'false'
 ;

Number
 : Int ('.' Digit*)?
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

EscapeSequence
   :   '\\' ('b'|'t'|'n'|'f'|'r'|'"'|'\''|'\\')
   |   UnicodeEscape
   |   OctalEscape
   ;

fragment
OctalEscape
   :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
   |   '\\' ('0'..'7') ('0'..'7')
   |   '\\' ('0'..'7')
   ;

fragment
UnicodeEscape
   :   '\\' 'u' HexDigit HexDigit HexDigit HexDigit
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
 : '-'? [1-9] Digit* [L]*
 | '0'
 ;

fragment Digit
 : [0-9]
 ;
